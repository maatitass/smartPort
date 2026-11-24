# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "paho-mqtt>=2.0.0",
# ]
# ///
"""Script di test per lo streaming AIS di Digitraffic Marine (MQTT over WSS)."""

import argparse
import json
import threading
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import paho.mqtt.client as mqtt

BROKER = "meri.digitraffic.fi"
PORT = 443
TOPIC = "vessels-v2/#"

lock = threading.Lock()
stats = {
    "counts": Counter(),
    "mmsis_by_kind": defaultdict(set),
    "parse_errors": 0,
    "quality_issues": defaultdict(Counter),
    "latencies_sec": defaultdict(list),
    "samples": {},
}


def on_connect(client, userdata, flags, reason_code, properties):
    if not reason_code.is_failure:
        print(f"Connesso a wss://{BROKER}:{PORT}/mqtt — sottoscrizione a '{TOPIC}'...")
        client.subscribe(TOPIC)
    else:
        print(f"Errore connessione MQTT: {reason_code}")


def on_message(client, userdata, msg):
    now = time.time()
    parts = msg.topic.split("/")
    mmsi = parts[1] if len(parts) == 3 else None
    kind = parts[-1]  # 'location', 'metadata', 'status', ...

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        with lock:
            stats["counts"][kind] += 1
            stats["parse_errors"] += 1
        return

    with lock:
        stats["counts"][kind] += 1
        if mmsi:
            stats["mmsis_by_kind"][kind].add(mmsi)

        if kind not in stats["samples"]:
            stats["samples"][kind] = payload

        if isinstance(payload, dict):
            for k, v in payload.items():
                if v is None or (isinstance(v, str) and not v.strip()):
                    stats["quality_issues"][kind][f"empty:{k}"] += 1

            if kind == "location":
                if isinstance(payload.get("time"), (int, float)):
                    stats["latencies_sec"][kind].append(now - payload["time"])
                if payload.get("heading") == 511:  # valore sentinella AIS = N/A
                    stats["quality_issues"][kind]["heading==511"] += 1
                lat, lon = payload.get("lat"), payload.get("lon")
                if not (
                    isinstance(lat, (int, float))
                    and isinstance(lon, (int, float))
                    and -90 <= lat <= 90
                    and -180 <= lon <= 180
                ):
                    stats["quality_issues"][kind]["invalid_coords"] += 1
            elif kind == "metadata":
                if isinstance(payload.get("timestamp"), (int, float)):
                    stats["latencies_sec"][kind].append(now - payload["timestamp"] / 1000.0)

        record = {"received_at": round(now, 3), "topic": msg.topic, "payload": payload}
        userdata["file"].write(json.dumps(record, ensure_ascii=False) + "\n")


def print_report(elapsed: float, output_file: Path):
    with lock:
        counts = dict(stats["counts"])
        total = sum(counts.values())
        loc_mmsis = stats["mmsis_by_kind"]["location"]
        meta_mmsis = stats["mmsis_by_kind"]["metadata"]
        all_mmsis = set().union(*stats["mmsis_by_kind"].values()) if stats["mmsis_by_kind"] else set()
        both_mmsis = loc_mmsis & meta_mmsis
        parse_errors = stats["parse_errors"]
        latencies = {k: sorted(v) for k, v in stats["latencies_sec"].items() if v}
        quality_issues = {k: dict(v) for k, v in stats["quality_issues"].items() if v}
        samples = dict(stats["samples"])

    print("\n" + "=" * 60)
    print(f"REPORT RACCOLTA ({elapsed:.1f}s)")
    print("=" * 60)
    print(f"Eventi totali:     {total} ({total / max(elapsed, 1):.2f} msg/sec)")
    print(f"Per tipo:          {counts}")
    print(
        f"Navi distinte:     {len(all_mmsis)} MMSI "
        f"(location={len(loc_mmsis)}, metadata={len(meta_mmsis)}, entrambi={len(both_mmsis)})"
    )
    print(f"Errori JSON:       {parse_errors}")

    if latencies:
        print("\nLatenza (received_at - event_time):")
        for kind, vals in latencies.items():
            p50 = vals[len(vals) // 2]
            p95 = vals[int(len(vals) * 0.95)]
            print(f"  [{kind}] mediana={p50:.2f}s | p95={p95:.2f}s | min={vals[0]:.2f}s | max={vals[-1]:.2f}s")

    if quality_issues:
        print("\nAnomalie / campi nulli, vuoti o sentinella:")
        for kind, issues in quality_issues.items():
            print(f"  [{kind}] {issues}")

    if samples:
        print("\nStruttura di esempio (1 evento per tipo):")
        for kind, sample in samples.items():
            print(f"  [{kind}] {json.dumps(sample, ensure_ascii=False)}")

    print(f"\nDati grezzi salvati in: {output_file.resolve()}")


def parse_args():
    parser = argparse.ArgumentParser(description="Test streaming MQTT Digitraffic Marine.")
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=600,
        help="Durata raccolta in secondi (default: 600 = 10 min)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("digitraffic_sample.jsonl"),
        help="File JSONL di output (default: digitraffic_sample.jsonl)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()

    with args.output.open("w", encoding="utf-8", buffering=1) as f:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"smartport-probe-{uuid.uuid4().hex[:8]}",
            transport="websockets",
            userdata={"file": f},
        )
        client.ws_set_options(path="/mqtt")
        client.tls_set()
        client.on_connect = on_connect
        client.on_message = on_message

        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()

        try:
            while (elapsed := time.time() - start) < args.duration:
                remaining = args.duration - elapsed
                time.sleep(min(10, max(remaining, 0)))
                now_elapsed = time.time() - start
                with lock:
                    total = sum(stats["counts"].values())
                    all_mmsis = (
                        len(set().union(*stats["mmsis_by_kind"].values()))
                        if stats["mmsis_by_kind"]
                        else 0
                    )
                print(
                    f"[{now_elapsed:5.0f}s/{args.duration}s] {total} eventi | "
                    f"{total / max(now_elapsed, 1):.1f} msg/s | {all_mmsis} navi"
                )
        except KeyboardInterrupt:
            print("\nInterrotto dall'utente.")
        finally:
            client.loop_stop()
            client.disconnect()

    print_report(time.time() - start, args.output)


if __name__ == "__main__":
    main()
