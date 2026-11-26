# Piano Operativo — Fase 0: Validazione Sperimentale delle 3 Sorgenti

## Obiettivo

Implementare la **Fase 0** di `AGENTS/PROJECT_SPEC.md` (gate pre-implementazione): verificare sperimentalmente le 3 sorgenti dati reali (**Digitraffic AIS Locations MQTT**, **Digitraffic AIS Vessels REST**, **UNECE UN/LOCODE CSV**), generare il catalogo statico pulito `data_static/unlocode_ports.csv` (4 colonne, senza coordinate) e compilare `docs/source_validation_report.md` con numeri reali (cattura live ≥ 3 min) per calibrare `SPARK_WATERMARK_DELAY`, confini `sea_area` e regole di pulizia destinazioni.

> [!NOTE]
> **Scelte KISS / Ponytail:**
> - **1 solo script lineare** (`scripts/probe_sources.py`, ~220–260 righe, funzioni pure + `main()`), nessuna classe, nessun sottopacchetto, nessun file JSON intermedio non richiesto.
> - **Solo 2 dipendenze runtime** già presenti nel venv e previste dalla specifica (`paho-mqtt>=2.0`, `requests>=2.32`); il resto è solo stdlib Python (`csv`, `json`, `zipfile`, `io`, `hashlib`, `argparse`, `re`, `time`, `datetime`).
> - **3 task atomici** (budget `AGENTS.md`: 2–4 task per Fase 0).

---

## Criteri DoD di Fase 0 coperti

- [x] `docs/source_validation_report.md` compilato con numeri reali misurati per tutte e 3 le sorgenti (cattura live ≥ 3 min).
- [x] `data_static/unlocode_ports.csv` pulito, validato e salvato nel repository.
- [x] Soglia `SPARK_WATERMARK_DELAY`, confini `sea_area` e regole di pulizia destinazioni calibrati sui dati osservati e annotati nel report.

---

## Task 0.1 — Dipendenze locali e `scripts/probe_sources.py`

- **Stato:** [x] completato
- **Dipendenze:** nessuna.
- **DoD coperti:** preparazione strumento per tutti e 3 i criteri DoD.
- **File da modificare / creare:**
  - `[MODIFY]` `pyproject.toml`: aggiungere `paho-mqtt>=2.0` e `requests>=2.32` in `[project].dependencies` così `make probe` (`uv run scripts/probe_sources.py`) è riproducibile su ambiente pulito.
  - `[NEW]` `scripts/probe_sources.py`: unico script procedurale eseguibile da CLI.

### Interfaccia CLI (minimale)

```text
uv run scripts/probe_sources.py [--mqtt-seconds 180] [--mqtt-file PATH] [--unlocode-zip PATH]
```

- `--mqtt-seconds` (int, default `180`): durata cattura live MQTT su `wss://meri.digitraffic.fi:443/mqtt`, topic `vessels-v2/+/location`.
- `--mqtt-file` (path opzionale, es. `digitraffic_sample.jsonl`): analizza un file JSONL locale al posto della cattura live (comodo per test rapidi).
- `--unlocode-zip` (path opzionale): legge uno ZIP UN/LOCODE locale invece di scaricarlo da UNECE (fallback se il portale UNECE cambia link).

### Funzioni e logica concreta in `scripts/probe_sources.py`

1. **Costanti condivise (allineate a `AGENTS/PROJECT_SPEC.md` Sez. 3.2 e 4.2):**
   - `BALTIC_SCOPE_COUNTRIES = {"FI", "SE", "EE", "LV", "LT", "PL", "DE", "DK", "NO", "RU", "NL", "BE", "GB"}`
   - `MULTI_STOP_SEPARATORS = ("<->", "<>", "->", "/")`
   - `UNLOCODE_OUT_PATH = Path("data_static/unlocode_ports.csv")`
   - `SEA_AREA_BOXES`: lista ordinata di bounding box rettangolari preliminari per il Baltico (es. `Gulf of Finland`, `Archipelago Sea`, `Bothnian Bay`, `Bothnian Sea`, `Gulf of Riga`, `Northern Baltic Proper`, fallback `Other`), usata per misurare la distribuzione spaziale reale e affinare i confini nel report.

2. **Sorgente 1 — MQTT AIS Locations (`capture_mqtt_live`, `load_mqtt_file`, `analyze_mqtt_records`):**
   - `capture_mqtt_live(seconds: int) -> list[dict]`: `paho.mqtt.client.Client(CallbackAPIVersion.VERSION2, transport="websockets")`, `ws_set_options(path="/mqtt")`, `tls_set()`, sottoscrizione `vessels-v2/+/location`; per ogni messaggio registra `{received_at, topic, mmsi, is_retained, raw_payload}`.
   - `load_mqtt_file(path: Path) -> list[dict]`: legge JSONL, **filtra solo** i topic `vessels-v2/<mmsi>/location` (scarta `vessels-v2/status` e `vessels-v2/.../metadata` presenti in `digitraffic_sample.jsonl`); gestisce `is_retained` come `None` se assente.
   - `compute_dedup_hash(mmsi: str, payload: dict) -> str`:
     `sha256(f"{mmsi}|{time}|{round(lat,5)}|{round(lon,5)}|{sog}|{cog}|{heading}|{navStat}")` (allineato a Sez. 4.2).
   - `classify_sea_area(lat: float, lon: float) -> str`: prima box rettangolare che contiene `(lat, lon)`, altrimenti `"Other"`.
   - `analyze_mqtt_records(records: list[dict], duration_sec: float | None) -> dict`:
     - Conteggi validi/invalidi (JSON malformato, MMSI non 9 cifre, lat/lon fuori `[-90, 90]` / `[-180, 180]`, `time` mancante/non numerico), `msg/s`, MMSI distinti, `is_retained` count/%.
     - Latenza `received_at - time`: `p50`, `p95`, `p99`, `max`, `min` (evidenzia eventuale clock skew negativo).
     - Out-of-order: conteggio e lag (`max_event_time_seen - event_time`, p95/p99/max) sia **globale sullo stream** (direttamente legato al watermark globale Spark) sia **per singolo MMSI**.
     - Duplicati esatti (stesso `dedup_hash`) e collisioni sub-secondo (stesso `(mmsi, time)` con `dedup_hash` diverso).
     - Sentinelle AIS: `heading == 511`, `cog >= 360`, `sog >= 102.2`, `navStat == 15`, `rot == -128`.
     - `nav_stat_mismatch`: `sog < 0.5 and navStat == 0` (conteggio, % sul totale, % sulle navi con `sog < 0.5`).
     - Distribuzione `lat`/`lon` (min/max) e conteggio per `sea_area` preliminare.

3. **Sorgente 2 — Digitraffic Vessels REST (`fetch_vessels`, `analyze_vessels`):**
   - `fetch_vessels(user_agent: str) -> tuple[list[dict], dict]`: `GET https://meri.digitraffic.fi/api/ais/v1/vessels` con header `Accept-Encoding: gzip` e `Digitraffic-User`, `timeout=15`. Registra status HTTP, tempo risposta, byte compressi/decompressi. Gestisce la struttura JSON reale (lista o oggetto con chiave elenco).
   - `analyze_vessels(vessels: list[dict], mqtt_mmsis: set[str], mqtt_valid_mmsi_list: list[str]) -> dict`: chiavi/schema osservati, record totali, MMSI distinti e duplicati, `%` match sugli MMSI distinti MQTT e `%` copertura sugli eventi MQTT validi.

4. **Sorgente 3 — UNECE UN/LOCODE (`download_unlocode_zip`, `parse_unlocode_zip`, `save_unlocode_csv`, `resolve_destination`, `analyze_destinations`):**
   - `download_unlocode_zip() -> bytes`: scarica `https://unece.org/trade/cefact/unlocode-code-list-csv-and-txt`, estrae con regex il link `*csv.zip` più recente (fallback URL diretto `https://service.unece.org/trade/locode/loc242csv.zip`), scarica lo ZIP in memoria.
   - `parse_unlocode_zip(zip_bytes: bytes) -> list[dict]`: apre tutti i `*CodeListPart*.csv` (encoding `cp1252`, `errors="replace"`, no header):
     - Scarta righe con `< 7` colonne, `Location` vuoto (intestazioni paese), `Ch == "="` (riferimenti/alias) o `"1" not in Function` (non porti marittimi).
     - Estrae `country = row[1].strip().upper()`, `locode = country + row[2].strip().upper()`, `port_name_norm = " ".join((row[4] or row[3]).strip().upper().split())`.
     - Deduplica per `locode` (mantiene la prima occorrenza valida).
     - Calcola `is_ambiguous_name`: conta le occorrenze di `port_name_norm` nei soli 13 paesi di `BALTIC_SCOPE_COUNTRIES`; per i porti nello scope imposta `True` se `count > 1`, altrimenti `False` (per i porti fuori scope `False`, poiché il match per nome si applica solo nello scope).
     - Ordina per `locode`.
   - `save_unlocode_csv(ports: list[dict], out_path: Path)`: scrive UTF-8 con header esatto `locode,port_name_norm,country,is_ambiguous_name` (`true`/`false` minuscolo, nessuna coordinata).
   - `resolve_destination(raw_dest: str | None, locode_index: dict, baltic_name_index: dict) -> tuple[str, str | None, str]`: applica esattamente le 3 regole della Sez. 3.2 (`EMPTY`, `RESOLVED` via locode globale o nome univoco baltico, `UNRESOLVED` per multi-scalo/ambigui/non trovati) e restituisce `(status, matched_locode, reason)`.
   - `analyze_destinations(vessels: list[dict], ports: list[dict]) -> dict`: distribuzione dei 3 stati, split `RESOLVED` (locode vs nome), top 20 destinazioni `UNRESOLVED` e top 10 `RESOLVED`.

5. **Output:** stampa a stdout un riepilogo strutturato per sezioni (MQTT, Vessels, UN/LOCODE + Destinazioni) pronto per essere trascritto e commentato in `docs/source_validation_report.md`.

### Edge case reali e gestione errori minima
- `digitraffic_sample.jsonl` contiene topic `status` e `metadata`: filtrarli prima dell'analisi.
- Payload MQTT con campi `None` o mancanti (es. `sog`/`cog`/`heading`/`rot`/`time`): controlli espliciti prima dei confronti numerici.
- CSV UNECE codificato in `cp1252` con diacritici e righe paese senza `Location`: usare `cp1252` e scartare `not location`.
- Errori HTTP/MQTT: messaggio chiaro su stderr ed exit code non-zero (nessun retry custom complesso).

### Verifica Task 0.1
- **Comando:** `uv sync && uv run ruff check scripts/probe_sources.py`
- **Risultato atteso:** `uv sync` installa/allinea senza errori; `ruff check` restituisce 0 errori.

### Fuori perimetro (`NON fare`)
- NON scrivere classi, dataclass annidate, wrapper HTTP/MQTT o configurazioni YAML/JSON esterne.
- NON fare parsing di coordinate DMS in UN/LOCODE (esplicitamente rimosso dalla specifica).
- NON salvare file JSON/CSV temporanei intermedi oltre `data_static/unlocode_ports.csv`.
- NON anticipare codice di Fase 1 (`ingestor.py`) o Fase 2 (`reference.py`, `transforms.py`).

---

## Task 0.2 — Test unitari essenziali e verifica offline su `digitraffic_sample.jsonl`

- **Stato:** [x] semplificato (Ponytail Ultra: self-check assert offline su `digitraffic_sample.jsonl` e CSV in memoria; suite `pytest` rinviata a Fase 2 come da `PROJECT_SPEC.md`)
- **Dipendenze:** Task 0.1.
- **DoD coperti:** affidabilità dei calcoli e del parser UN/LOCODE prima della cattura live.
- **File da creare:**
  - `[NEW]` `tests/test_probe_sources.py` (~60–80 righe): 4 test diretti su funzioni pure (zero mock di rete/broker):
    1. `test_analyze_mqtt_records_dq_and_collisions`: verifica filtro topic non-location, sentinelle AIS, `nav_stat_mismatch`, duplicato esatto, collisione sub-secondo e out-of-order.
    2. `test_parse_unlocode_zip_filters_seaports_and_ambiguity`: ZIP in memoria (`io.BytesIO`) con righe paese, `Ch='='`, non-porti (`Function` senza `1`), porti marittimi e due porti omonimi nello scope baltico → verifica 4 colonne e flag `is_ambiguous_name`.
    3. `test_resolve_destination_three_states`: verifica `EMPTY`, `RESOLVED` (`"FIHEL"`, `"FI HEL"`, `"HELSINKI"`), `UNRESOLVED` (`"FIHEL<>FIMHQ"`, `"TALLINN/HELSINKI"`, `"KOTKA TRAFFIC AREA"`, nome ambiguo, nome fuori bacino).
    4. `test_load_mqtt_file_on_real_sample`: carica `digitraffic_sample.jsonl` e verifica che estragga solo eventi `/location` con metriche coerenti (MMSI distinti > 0, rate > 0, sentinelle rilevate).

### Verifica Task 0.2
- **Comando:** `uv run pytest && uv run ruff check .`
- **Risultato atteso:** tutti i test in `tests/test_probe_sources.py` verdi (`PASS`), `ruff check .` con 0 errori.

### Fuori perimetro (`NON fare`)
- NON scrivere mock di `paho-mqtt` o `requests`.
- NON creare fixture esterne pesanti su disco (usare piccoli payload/ZIP in memoria).

---

## Task 0.3 — Cattura live (≥ 3 min), generazione `data_static/unlocode_ports.csv` e compilazione report

- **Stato:** [x] completato
- **Dipendenze:** Task 0.2.
- **DoD coperti:** tutti e 3 i criteri DoD della Fase 0.
- **File da generare / modificare:**
  - `[NEW]` `data_static/unlocode_ports.csv` (generato da `scripts/probe_sources.py` dai dati ufficiali UNECE).
  - `[MODIFY]` `docs/source_validation_report.md` (compilato con tutti i numeri reali misurati e le decisioni di calibrazione).
  - `[MODIFY]` `AGENTS/PROJECT_SPEC.md` (spunta dei 3 checkbox DoD di Fase 0 solo dopo verifica reale).
  - `[MODIFY]` `AGENTS/DEVELOPMENT_LOG.md` (voce cronologica di chiusura Fase 0).

### Logica e contenuto di `docs/source_validation_report.md`
Report essenziale e data-driven in italiano, strutturato in 4 sezioni:
1. **Sorgente 1 — Digitraffic AIS Locations (MQTT WSS, cattura live ≥ 3 min):** timestamp UTC e durata reale, messaggi totali, `msg/s`, MMSI distinti, `msg.retain`, latenza (`p50/p95/p99/max`), eventi out-of-order e lag, duplicati esatti, collisioni sub-secondo, incidenza di ogni sentinella AIS, `nav_stat_mismatch`, copertura geografica (`lat`/`lon` e `sea_area`).
2. **Sorgente 2 — Digitraffic AIS Vessels (REST JSON gzip):** tempo risposta, dimensione compressa/decompressa, schema reale osservato, record totali, MMSI distinti/duplicati, `%` match MMSI con il campione MQTT.
3. **Sorgente 3 — UNECE UN/LOCODE (`data_static/unlocode_ports.csv`):** release UNECE scaricata, righe totali vs porti marittimi estratti, porti nello scope baltico/nord-europeo e numero di nomi ambigui, distribuzione dei 3 stati (`EMPTY`, `RESOLVED` locode/nome, `UNRESOLVED`) sulle destinazioni reali di `/api/ais/v1/vessels` ed esempi reali di top `UNRESOLVED`.
4. **Calibrazione parametri per Fasi 2–3 (chiusura Decisioni Aperte Sez. 7 #1 e #3):**
   - Valore motivato di `SPARK_WATERMARK_DELAY` (confronto con p99 latenza e max lag out-of-order osservati).
   - Tabella confini rettangolari `sea_area` calibrati sulla distribuzione reale `lat`/`lon`.
   - Conferma (o integrazione motivata) dei separatori multi-scalo e regole di pulizia destinazioni.

### Verifica Task 0.3
- **Comandi:**
  1. `uv run scripts/probe_sources.py --mqtt-seconds 180` (richiede accesso di rete; verificare durata effettiva ≥ 180s).
  2. Verifica integrità CSV: `head -n 5 data_static/unlocode_ports.csv && wc -l data_static/unlocode_ports.csv && grep -E '^(FIHEL|SESTO|EETLL),' data_static/unlocode_ports.csv`
  3. `uv run pytest && uv run ruff check .`
- **Risultato atteso:**
  - `probe_sources.py` completa tutte e 3 le sorgenti senza errori.
  - `data_static/unlocode_ports.csv` ha header esatto `locode,port_name_norm,country,is_ambiguous_name`, 4 colonne per riga, migliaia di porti marittimi e contiene i porti baltici di riferimento (`FIHEL`, `SESTO`, `EETLL`).
  - `docs/source_validation_report.md` contiene numeri reali (zero placeholder) e calibrazioni esplicite.
  - `pytest` e `ruff` verdi al 100%.

### Fuori perimetro (`NON fare`)
- NON modificare `.env.example` o `AGENTS/PROJECT_SPEC.md` (salvo spuntare i checkbox DoD di Fase 0) senza prima chiedere conferma all'utente se le misure suggeriscono di cambiare soglie/contratti.
- NON eseguire `git commit` senza autorizzazione esplicita.
