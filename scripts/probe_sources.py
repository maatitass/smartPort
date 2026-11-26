"""Phase 0 probe: validate Digitraffic MQTT, Vessels REST, and UNECE UN/LOCODE."""

import argparse
import csv
import hashlib
import io
import json
import time
from collections import Counter
from pathlib import Path

import paho.mqtt.client as mqtt
import requests

MQTT_BROKER = "meri.digitraffic.fi"
MQTT_TOPIC = "vessels-v2/+/location"
VESSELS_URL = "https://meri.digitraffic.fi/api/ais/v1/vessels"
DIGITRAFFIC_USER = "smartport-lsdm-university-project"
UNLOCODE_CSV_URL = "https://raw.githubusercontent.com/datasets/un-locode/main/data/code-list.csv"
UNLOCODE_OUT = Path("data_static/unlocode_ports.csv")

BALTIC_COUNTRIES = {"FI", "SE", "EE", "LV", "LT", "PL", "DE", "DK", "NO", "RU", "NL", "BE", "GB"}
MULTI_STOP_SEPS = ("<->", "<>", "->", "/")
SEA_AREAS = (
    ("Gulf of Finland", 59.3, 60.8, 22.9, 30.3),
    ("Archipelago Sea", 59.5, 60.8, 19.5, 22.9),
    ("Bothnian Bay", 63.5, 66.0, 20.0, 26.0),
    ("Bothnian Sea", 60.8, 63.5, 17.0, 22.5),
    ("Gulf of Riga", 56.9, 58.6, 22.5, 24.8),
    ("Northern Baltic Proper", 57.0, 59.5, 17.5, 22.9),
    ("Southern Baltic", 53.5, 57.0, 10.0, 21.5),
)


def is_num(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def pct(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    return round(sorted_vals[int(round(p / 100 * (len(sorted_vals) - 1)))], 3)


def extract_mmsi(topic: str) -> str | None:
    parts = topic.split("/")
    return (
        parts[1]
        if len(parts) == 3 and parts[0] == "vessels-v2" and parts[2] == "location"
        else None
    )


def dedup_hash(mmsi: str, p: dict) -> str:
    raw = (
        f"{mmsi}|{p['time']}|{round(float(p['lat']), 5)}|{round(float(p['lon']), 5)}|"
        f"{p.get('sog')}|{p.get('cog')}|{p.get('heading')}|{p.get('navStat')}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def sea_area(lat: float, lon: float) -> str:
    for name, lat0, lat1, lon0, lon1 in SEA_AREAS:
        if lat0 <= lat <= lat1 and lon0 <= lon <= lon1:
            return name
    return "Other"


def capture_mqtt_live(seconds: int) -> tuple[list[dict], float]:
    records: list[dict] = []
    connect_err: list[str] = []

    def on_connect(client, _u, _f, rc, _props):
        if rc == 0:
            client.subscribe(MQTT_TOPIC)
        else:
            connect_err.append(f"MQTT connect failed: {rc}")

    def on_message(_c, _u, msg):
        mmsi = extract_mmsi(msg.topic)
        if mmsi:
            records.append(
                {
                    "received_at": time.time(),
                    "mmsi": mmsi,
                    "is_retained": bool(msg.retain),
                    "raw_payload": msg.payload.decode("utf-8", errors="replace"),
                }
            )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    client.ws_set_options(path="/mqtt")
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 443, keepalive=60)
    t0 = time.monotonic()
    client.loop_start()
    try:
        while time.monotonic() - t0 < seconds:
            if connect_err:
                raise RuntimeError(connect_err[0])
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()
    return records, time.monotonic() - t0


def load_mqtt_file(path: Path) -> tuple[list[dict], float | None]:
    records, times = [], []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        mmsi = extract_mmsi(str(item.get("topic", "")))
        if not mmsi:
            continue
        rx = item.get("received_at")
        if is_num(rx):
            times.append(float(rx))
        raw = item.get("raw_payload", item.get("payload"))
        records.append(
            {
                "received_at": rx,
                "mmsi": mmsi,
                "is_retained": item.get("is_retained"),
                "raw_payload": raw,
            }
        )
    duration = (max(times) - min(times)) if len(times) >= 2 else None
    return records, duration


def analyze_mqtt(records: list[dict], duration_sec: float | None = None) -> tuple[dict, list[str]]:
    sentinels: Counter[str] = Counter()
    areas: Counter[str] = Counter()
    valid_mmsis, latencies, ooo_lags, lats, lons = [], [], [], [], []
    seen_hash, seen_mmsi_t = set(), set()
    max_t: float | None = None
    invalid = retained = exact_dups = subsec_colls = low_sog = mismatch = 0

    for rec in records:
        try:
            p = (
                rec["raw_payload"]
                if isinstance(rec["raw_payload"], dict)
                else json.loads(rec["raw_payload"])
            )
        except (TypeError, json.JSONDecodeError):
            invalid += 1
            continue
        if not isinstance(p, dict):
            invalid += 1
            continue
        mmsi, lat, lon, ts = str(rec.get("mmsi", "")), p.get("lat"), p.get("lon"), p.get("time")
        if not (
            mmsi.isdigit()
            and len(mmsi) == 9
            and is_num(lat)
            and is_num(lon)
            and -90 <= lat <= 90
            and -180 <= lon <= 180
            and is_num(ts)
            and ts > 0
        ):
            invalid += 1
            continue

        valid_mmsis.append(mmsi)
        retained += int(rec.get("is_retained") is True)
        if is_num(rec.get("received_at")):
            latencies.append(float(rec["received_at"]) - float(ts))

        t = float(ts)
        if max_t is not None and t < max_t:
            ooo_lags.append(max_t - t)
        else:
            max_t = t

        h = dedup_hash(mmsi, p)
        if h in seen_hash:
            exact_dups += 1
        else:
            seen_hash.add(h)
            if (mmsi, t) in seen_mmsi_t:
                subsec_colls += 1
            seen_mmsi_t.add((mmsi, t))

        sog, cog, hdg, nav, rot = (
            p.get("sog"),
            p.get("cog"),
            p.get("heading"),
            p.get("navStat"),
            p.get("rot"),
        )
        if is_num(hdg) and hdg == 511:
            sentinels["heading==511"] += 1
        if is_num(cog) and cog >= 360:
            sentinels["cog>=360"] += 1
        if is_num(sog) and sog >= 102.2:
            sentinels["sog>=102.2"] += 1
        if is_num(nav) and nav == 15:
            sentinels["navStat==15"] += 1
        if is_num(rot) and rot == -128:
            sentinels["rot==-128"] += 1
        if is_num(sog) and sog < 0.5:
            low_sog += 1
            mismatch += int(is_num(nav) and nav == 0)

        lats.append(float(lat))
        lons.append(float(lon))
        areas[sea_area(float(lat), float(lon))] += 1

    latencies.sort()
    ooo_lags.sort()
    n = len(valid_mmsis)
    stats = {
        "total": len(records),
        "valid": n,
        "invalid": invalid,
        "duration_sec": round(duration_sec, 1) if duration_sec else None,
        "msg_per_sec": round(len(records) / duration_sec, 2) if duration_sec else None,
        "distinct_mmsi": len(set(valid_mmsis)),
        "retained": retained,
        "latency_s": {
            k: pct(latencies, q) for k, q in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))
        },
        "ooo_count": len(ooo_lags),
        "ooo_lag_s": {
            "p95": pct(ooo_lags, 95),
            "p99": pct(ooo_lags, 99),
            "max": pct(ooo_lags, 100),
        },
        "exact_duplicates": exact_dups,
        "subsecond_collisions": subsec_colls,
        "sentinels": dict(sentinels),
        "nav_stat_mismatch": mismatch,
        "low_sog_count": low_sog,
        "lat_range": [round(min(lats), 4), round(max(lats), 4)] if lats else None,
        "lon_range": [round(min(lons), 4), round(max(lons), 4)] if lons else None,
        "sea_areas": dict(areas.most_common()),
    }
    return stats, valid_mmsis


def fetch_vessels() -> tuple[list[dict], dict]:
    t0 = time.monotonic()
    r = requests.get(
        VESSELS_URL,
        headers={"Accept-Encoding": "gzip", "Digitraffic-User": DIGITRAFFIC_USER},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    vessels = data if isinstance(data, list) else [f.get("properties", f) for f in data["features"]]
    meta = {
        "elapsed_s": round(time.monotonic() - t0, 2),
        "encoding": r.headers.get("Content-Encoding"),
        "decoded_bytes": len(r.content),
    }
    return vessels, meta


def analyze_vessels(vessels: list[dict], mqtt_mmsis: list[str]) -> dict:
    v_mmsis = [str(v["mmsi"]) for v in vessels if v.get("mmsi") is not None]
    v_set, m_set = set(v_mmsis), set(mqtt_mmsis)
    keys = sorted({k for v in vessels for k in v})
    matched_m = len(m_set & v_set)
    matched_ev = sum(1 for m in mqtt_mmsis if m in v_set)
    return {
        "records": len(vessels),
        "distinct_mmsi": len(v_set),
        "duplicate_mmsi": len(v_mmsis) - len(v_set),
        "schema_keys": keys,
        "mqtt_mmsi_match_pct": round(100 * matched_m / len(m_set), 2) if m_set else 0.0,
        "mqtt_event_match_pct": round(100 * matched_ev / len(mqtt_mmsis), 2) if mqtt_mmsis else 0.0,
    }


def parse_unlocode_csv(csv_text: str) -> list[dict]:
    rows, seen = [], set()
    for r in csv.DictReader(io.StringIO(csv_text)):
        if (r.get("Change") or "").strip() == "=":
            continue
        country = (r.get("Country") or "").strip().upper()
        loc = (r.get("Location") or "").strip().upper()
        func = (r.get("Function") or "").strip()
        if len(country) != 2 or len(loc) != 3 or "1" not in func:
            continue
        locode = country + loc
        if locode in seen:
            continue
        seen.add(locode)
        raw_name = (r.get("NameWoDiacritics") or r.get("Name") or "").strip()
        port_name = " ".join(raw_name.upper().split())
        rows.append({"locode": locode, "port_name_norm": port_name, "country": country})

    baltic_counts = Counter(p["port_name_norm"] for p in rows if p["country"] in BALTIC_COUNTRIES)
    return [
        {
            **p,
            "is_ambiguous_name": p["country"] in BALTIC_COUNTRIES
            and baltic_counts[p["port_name_norm"]] > 1,
        }
        for p in sorted(rows, key=lambda p: p["locode"])
    ]


def save_unlocode_csv(ports: list[dict], out_path: Path = UNLOCODE_OUT) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["locode", "port_name_norm", "country", "is_ambiguous_name"])
        for p in ports:
            w.writerow(
                [
                    p["locode"],
                    p["port_name_norm"],
                    p["country"],
                    str(p["is_ambiguous_name"]).lower(),
                ]
            )


def resolve_destination(
    raw_dest: str | None, by_code: dict[str, dict], by_name: dict[str, dict]
) -> tuple[str, str | None]:
    if not raw_dest or not raw_dest.strip():
        return "EMPTY", None
    cleaned = " ".join(raw_dest.strip().upper().split())
    if any(sep in cleaned for sep in MULTI_STOP_SEPS):
        return "UNRESOLVED", None
    compact = cleaned.replace(" ", "")
    if len(compact) == 5 and compact in by_code:
        return "RESOLVED", compact
    if cleaned in by_name:
        return "RESOLVED", by_name[cleaned]["locode"]
    return "UNRESOLVED", None


def analyze_destinations(vessels: list[dict], ports: list[dict]) -> dict:
    by_code = {p["locode"]: p for p in ports}
    by_name = {
        p["port_name_norm"]: p
        for p in ports
        if p["country"] in BALTIC_COUNTRIES and not p["is_ambiguous_name"]
    }
    seen, statuses, unresolved = set(), Counter(), Counter()
    for v in vessels:
        mmsi = str(v.get("mmsi", ""))
        if mmsi and mmsi in seen:
            continue
        seen.add(mmsi)
        status, _ = resolve_destination(v.get("destination"), by_code, by_name)
        statuses[status] += 1
        if status == "UNRESOLVED" and v.get("destination"):
            unresolved[" ".join(str(v["destination"]).strip().upper().split())] += 1
    total = sum(statuses.values())
    return {
        "vessels_evaluated": total,
        "status_counts": dict(statuses),
        "status_pct": {k: round(100 * c / total, 2) for k, c in statuses.items()} if total else {},
        "top_unresolved": unresolved.most_common(15),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Phase 0 probe for Smart Port's 3 data sources.")
    ap.add_argument("--mqtt-seconds", type=int, default=180)
    ap.add_argument("--mqtt-file", type=Path, default=None)
    ap.add_argument("--unlocode-csv", type=Path, default=None)
    args = ap.parse_args()

    # Fetch static HTTP sources first so network/URL errors fail fast before the MQTT capture.
    vessels, v_meta = fetch_vessels()
    if args.unlocode_csv:
        csv_text, unlocode_src = (
            args.unlocode_csv.read_text(encoding="utf-8"),
            str(args.unlocode_csv),
        )
    else:
        r = requests.get(UNLOCODE_CSV_URL, timeout=30)
        r.raise_for_status()
        csv_text, unlocode_src = r.text, UNLOCODE_CSV_URL

    ports = parse_unlocode_csv(csv_text)
    save_unlocode_csv(ports)

    records, dur = (
        load_mqtt_file(args.mqtt_file) if args.mqtt_file else capture_mqtt_live(args.mqtt_seconds)
    )
    mqtt_stats, valid_mmsis = analyze_mqtt(records, dur)

    report = {
        "1_mqtt": mqtt_stats,
        "2_vessels": {**v_meta, **analyze_vessels(vessels, valid_mmsis)},
        "3_unlocode": {
            "source": unlocode_src,
            "seaports_total": len(ports),
            "baltic_ports": sum(p["country"] in BALTIC_COUNTRIES for p in ports),
            "baltic_ambiguous": sum(p["is_ambiguous_name"] for p in ports),
            **analyze_destinations(vessels, ports),
        },
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
