# Smart Port — Registro di Sviluppo

Registro cronologico operativo del progetto (riferimento architetturale: `PROJECT_SPEC.md`).

---

## 2026-11-24 — Setup iniziale, sonda MQTT preliminare e definizione specifica

- **Modulo:** Pre-Modulo 0 (Setup & Specifica)
- **Task:** Test preliminare stream Digitraffic MQTT, definizione architettura ground truth (`PROJECT_SPEC.md`) e regole operative (`AGENTS.md`).
- **Stato:** completato
- **Attività svolte:**
  - Creato `test_digitraffic.py` (probe MQTT over WSS verso `meri.digitraffic.fi:443/mqtt`) e aggiornato `.gitignore` (`*.jsonl`).
  - Analizzato campione live di 30s (`digitraffic_sample.jsonl`: 1.048 eventi, 35.3 msg/s, 342 MMSI, latenza mediana 0.54s, sentinelle AIS, `navStat=0` sull'80% delle navi con `sog<0.5`, 4 collisioni sub-secondo `(mmsi, time)`).
  - Creato `PROJECT_SPEC.md` (Single Source of Truth v2, revisionata su 16 punti critici e divisa in Moduli 0–7).
  - Creato `AGENTS.md` e inizializzato `DEVELOPMENT_LOG.md`.
- **Decisioni tecniche:**
  - Stack multi-arch ARM64/AMD64: Apache Kafka 3.9 KRaft, Spark 3.5.5 Standalone (1 Master + 2 Worker, `shuffle.partitions=4`), Elasticsearch 8.17.4 (security ON, indici time-based + ILM delete-only, nessun rollover), Kibana 8.17.4, scrittura Spark→ES via `foreachPartition` + `elasticsearch-py`.
  - Data Integration nel nucleo su 4 sorgenti eterogenee: Digitraffic AIS MQTT, Digitraffic Vessels REST, UNECE UN/LOCODE CSV (Entity Resolution conservativa), Open-Meteo Weather & Marine (join spazio-temporale su 8–10 zone baltiche).
  - Separazione stateful Spark 3.5: dedup esatti (`dedup_hash`) nello Stadio 1 (`append`), aggregazioni a finestra 5m nello Stadio 2 (`update`) via Kafka Silver `ais.locations.enriched`.
  - Semplificazioni anti-overengineering nel nucleo: Parquet archivio rimandato a estensioni, JSONL in `context-sync` (niente `pyarrow`), `kafka-python-ng` pure Python, niente `geo_shape`, niente Caddy nel compose base.
- **Verifiche:**
  - `python3 -m py_compile test_digitraffic.py` → **PASS**
  - `uv run test_digitraffic.py --duration 5` (fuori sandbox) → **PASS** (170 eventi, 33.82 msg/s, 0 errori JSON)
  - Analisi campione 30s (`digitraffic_sample.jsonl`) → **PASS**
  - `uv run pytest` / `uv run ruff check` → **NOT RUN** (non ancora configurati)
- **Definition of Done:** Modulo 0 non ancora iniziato (tutti i criteri `[ ]` aperti in `PROJECT_SPEC.md`).
- **Blocker e prossimo passo:**
  - Nota ambiente locale: `docker` non presente nel `PATH` (richiesto dal Modulo 1 in poi; consigliato OrbStack o Colima con 8–10 GB RAM dedicati).
  - **Prossimo passo:** avviare **Modulo 0** (`scripts/probe_sources.py`: cattura MQTT 15 min + verifica live `/api/ais/v1/vessels`, UNECE UN/LOCODE CSV e Open-Meteo Weather/Marine → `docs/source_validation_report.md`).

---

## 2026-11-24 — Creazione struttura modulare e boilerplate iniziale del repository

- **Modulo:** Pre-Modulo 0 (Scaffolding)
- **Task:** Creazione struttura cartelle, configurazioni boilerplate (`.env.example`, `Makefile`, `pyproject.toml`, `docker-compose.yml`, `.gitignore`, `README.md`) e stub dei Moduli 0–7 senza logica applicativa.
- **Stato:** completato
- **Attività svolte:**
  - Aggiornati `.gitignore` (aggiunti `.env`, `data/`, `checkpoints/`, cache e eccezione `!services/replay/fixtures/*.jsonl`), `pyproject.toml` (configurazione `pytest` e `ruff`) e `README.md`.
  - Creati `.env.example`, `Makefile` e scheletro `docker-compose.yml`.
  - Creati i package e file stub (solo docstring/header, nessuna implementazione) per `scripts/`, `data_static/`, `infra/`, `services/{ingestor,context_sync,spark_app,replay}`, `kibana/`, `tests/` e `docs/`.
- **Decisioni tecniche:**
  - Inseriti `.gitkeep` nelle directory asset vuote (`data_static/`, `infra/es/ilm/`, `infra/es/templates/`, `services/replay/fixtures/`, `kibana/dashboards/`) per mantenerle tracciate in Git.
  - Esclusi dal `.gitignore` i soli fixture JSONL in `services/replay/fixtures/` preservando l'esclusione dei dump campionati (`*.jsonl`).
- **Verifiche:**
  - `python3 -m compileall -q scripts services tests` → **PASS**
  - `bash -n infra/kafka/init-topics.sh infra/es/init-es.sh kibana/export_dashboards.sh kibana/import_dashboards.sh` → **PASS**
  - `uv run pytest` / `uv run ruff check` → **NOT RUN** (solo scheletro senza test né pacchetti dev sincronizzati offline)
- **Definition of Done:** Modulo 0 non ancora iniziato; struttura pronta per avviare la pianificazione del Modulo 0.
- **Blocker e prossimo passo:** proporre `PLAN.md` per il **Modulo 0** (`scripts/probe_sources.py`) e attendere approvazione prima di implementare.

---

## 2026-11-24 — Aggiornamento strategia UN/LOCODE (codici globali + nomi regionali)

- **Modulo:** Pre-Modulo 0 (Specifica)
- **Task:** Aggiornamento della strategia di Entity Resolution UN/LOCODE in `AGENTS/PROJECT_SPEC.md` (approccio ibrido: porti marittimi globali per codici a 5 caratteri, perimetro Baltico/Nord Europa per nomi testuali liberi).
- **Stato:** completato
- **Attività svolte:**
  - Aggiornato `AGENTS/PROJECT_SPEC.md` sostituendo `data_static/unlocode_ports_baltic_eu.csv` con `data_static/unlocode_ports.csv` (tutti i porti marittimi globali con coordinate valide).
  - Definiti i campi `is_nordic_baltic_scope` (`FI, SE, EE, LV, LT, PL, DE, DK, NO, RU, NL, BE, GB`) e `is_ambiguous_name` in `ports_builder.py` e le regole distinte in `entity_resolution.py` (lookup globale per `RESOLVED_EXACT_LOCODE`/`RESOLVED_SPACED_LOCODE`, lookup circoscritto per `RESOLVED_UNAMBIGUOUS_NAME`).
- **Decisioni tecniche:**
  - Mantenuto Digitraffic Marine come sorgente AIS (scartato AISStream.io globale per rischi di affidabilità/outage, assenza di API REST per anagrafica, incompatibilità con integrazione meteo mirata e rischio saturazione risorse locali).
  - Adottato CSV UN/LOCODE globale sui soli porti marittimi (~1 MB) per non perdere rotte certe verso porti europei/extra-UE (`NLRTM`, `BEANR`, `DEHAM`, ecc.), limitando il matching per nome testuale al bacino Baltico/Nord Europa per evitare falsi positivi da omonimie globali.
- **Verifiche:**
  - `grep` e `git diff AGENTS/PROJECT_SPEC.md` → **PASS** (zero riferimenti residui a `unlocode_ports_baltic_eu.csv`)
- **Definition of Done:** Modulo 0 pronto per iniziare.
- **Blocker e prossimo passo:** avviare la pianificazione (`PLAN.md`) o l'implementazione del **Modulo 0** su richiesta dell'utente.
