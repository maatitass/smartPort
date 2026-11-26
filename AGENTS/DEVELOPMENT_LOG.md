# Smart Port — Registro di Sviluppo

Registro cronologico operativo del progetto (riferimento architetturale: `AGENTS/PROJECT_SPEC.md`).

---

## 2026-11-25 — Setup iniziale, specifica architetturale, governance e struttura repository

- **Modulo:** Pre-Modulo 0 (Setup, Specifica & Scaffolding)
- **Task:** Verifica preliminare Digitraffic MQTT, definizione ground truth architetturale (`AGENTS/PROJECT_SPEC.md`), regole operative (`AGENTS.md`) e scheletro modulare del repository.
- **Stato:** completato
- **Attività svolte:**
  - Eseguita sonda preliminare live su Digitraffic MQTT (`30s`: 1.048 eventi, ~35.3 msg/s, 342 MMSI, latenza mediana 0.54s, sentinelle AIS `heading=511`/`rot=-128`/`cog>=360`/`sog>=102.2`, `navStat=0` sull'80% delle navi con `sog<0.5`, 4 collisioni sub-secondo `(mmsi, time)`).
  - Definita la specifica tecnica definitiva in `AGENTS/PROJECT_SPEC.md` (Moduli 0–7, schemi Bronze/Silver/Gold, contratti I/O, DoD e invarianti della Sezione 7).
  - Definito `AGENTS.md` alla root del repository con principio KISS / Rasoio di Occam, workflow Planner (`PLAN.md`) → Executor, convenzioni, sicurezza e regole del registro di sviluppo.
  - Creati i file boilerplate root (`.env.example`, `.gitignore`, `Makefile`, `pyproject.toml`, `docker-compose.yml`, `README.md`) e la struttura cartelle modulare (`scripts/`, `data_static/`, `infra/`, `services/`, `kibana/`, `tests/`, `docs/`).
- **Decisioni tecniche:**
  - **Stack multi-arch (ARM64/AMD64, budget ≤ 6.5–7 GB RSS):** Apache Kafka 3.9 KRaft, Spark 3.5.5 Standalone (1 Master + 2 Worker, `shuffle.partitions=4`), Elasticsearch 8.17.4 (security ON, indici time-based + ILM delete-only, nessun rollover), Kibana 8.17.4, scrittura Spark→ES via `foreachPartition` + `elasticsearch-py` e scripted upsert Painless minimale (3 righe) su `ais-vessels-latest`.
  - **Data Integration (4 sorgenti eterogenee):** Digitraffic AIS MQTT, Digitraffic Vessels REST, UNECE UN/LOCODE CSV (porti marittimi globali per codici a 5 caratteri + nomi liberi circoscritti a Baltico/Nord Europa, 5 stati conservativi), Open-Meteo Weather & Marine (8–10 zone baltiche).
  - **Separazione stateful Spark 3.5 & KISS:** dedup esatti (`dedup_hash`) nello Stadio 1 (`append`), aggregazioni a finestra 5m (`groupBy(window, sea_area, ship_category)` e griglia Lat/Lon `0.05° × 0.10°`) nello Stadio 2 (`update`) via Kafka Silver `ais.locations.enriched`; `ports.jsonl` statico, `vessels`/`weather` con `CURRENT` (ultimi 3 snapshot), `kafka-python-ng` pure Python, niente `pyarrow`/`geo_shape`/Parquet/Caddy nel nucleo.
- **Verifiche:**
  - Sonda preliminare MQTT Digitraffic (`5s` + `30s`) → **PASS**
  - Controllo coerenza `AGENTS/PROJECT_SPEC.md`, `AGENTS.md`, `.env.example` e struttura cartelle (`git status --short`) → **PASS**
  - `uv run pytest` / `uv run ruff check` → **NOT RUN** (nessun modulo applicativo ancora implementato)
- **Definition of Done:** Modulo 0 non ancora iniziato (D1, D2, D3 aperti in `AGENTS/PROJECT_SPEC.md`).
- **Blocker e prossimo passo:**
  - Nota ambiente locale: `docker` non presente nel `PATH` (richiesto dal Modulo 1 in poi; consigliato OrbStack o Colima con 8–10 GB RAM).
  - **Prossimo passo:** pianificare e implementare il **Modulo 0** (`scripts/probe_sources.py`, `data_static/unlocode_ports.csv`, `docs/source_validation_report.md`) in modo snello secondo le regole KISS.

---

## 2026-11-25 — Semplificazione architetturale KISS della specifica e dello scheletro

- **Modulo/Fase:** Pre-Fase 0 (Revisione Specifica & Pulizia Scheletro)
- **Task:** Revisione completa di `AGENTS/PROJECT_SPEC.md` in chiave KISS senza perdita di requisiti, funzionalità visibili o correttezza analitica; riallineamento di `AGENTS.md`, configurazioni root e scheletro del repository.
- **Stato:** completato
- **Attività svolte:**
  - Riscritta `AGENTS/PROJECT_SPEC.md` separando rigorosamente **requisiti effettivi del corso** (Sez. 1.2) da **scelte progettuali/tecnologiche/funzionali** (Sez. 1.3) ed **estensioni non bloccanti** (Sez. 1.4, inclusi deploy cloud e verifica multi-arch; Docker Compose locale unico ambiente obbligatorio).
  - Riorganizzata la roadmap da 8 moduli a cascata (0→7) a **6 fasi incrementali end-to-end (Fasi 0→5)** con vertical slice live già in Fase 2 e sezione esplicita **Decisioni Aperte** (Sez. 7) da chiudere sperimentalmente.
  - Semplificata la Fase 0 (`scripts/probe_sources.py` unico script lineare, cattura default 3 min configurabile, solo `vessels-v2/+/location`, CSV UN/LOCODE pulito generato direttamente in Fase 0, riuso opzionale di campioni locali).
  - Aggiornati `AGENTS.md`, `.env.example`, `Makefile` (aggiunto `make reset-es` manuale), `README.md`, `.gitignore`, `docker-compose.yml` e consolidati gli script boilerplate (`infra/init-kafka.sh`, `infra/init-es.sh`, `kibana/{export,import}.sh`, `docs/technical_notes.md`).
  - Rimossi i boilerplate vuoti obsoleti (`main.py`, `services/context_sync/`, `services/replay/`, `infra/kafka/`, `infra/es/`, `kibana/{export,import}_dashboards.sh`, `docs/{data_dictionary,architecture_decisions,cloud_deploy}.md`, stub `NOT RUN` in `data/source_validation/`), preservando `digitraffic_sample.jsonl`, `AGENTS/REVIEW.md` e `docs/source_validation_report.md`.
- **Decisioni tecniche:**
  - **Context sync integrato nel driver Spark (`services/spark_app/context.py`):** eliminato il container separato `context-sync`, i 7 micro-file e il protocollo `CURRENT`/retention 3 snapshot; refresh fail-soft ogni 60 min con timeout HTTP `(5s, 10s)`, cooldown retry `300s` su `last_attempt` e fallback su cache locale `/data/reference/` o DataFrame vuoto tipizzato (pipeline mai bloccata).
  - **Deduplicazione a stato limitato (`dropDuplicatesWithinWatermark`):** adottato l'operatore nativo Spark 3.5 (SPARK-42931) con `watermark_time = event_time` sui validi e `watermark_time = received_at` (chiave univoca topic/partition/offset) sugli invalidi, evitando sia crescita illimitata dello stato in RAM sia drop silenzioso dei record di quarantena.
  - **Topic `ais.locations.enriched` mantenuto ma snellito (10 campi Gold-only):** risolve il divieto di chaining stateful in `update` mode e consente il refresh orario dei broadcast join prima di Gold; analizzato e documentato l'impatto dei retry Kafka (impatto zero da ingestor grazie a dedup Silver; su crash di Stadio 1 prima del commit checkpoint, `distinct_vessels_est` resta invariante al 100%, medie trascurabili, possibile lieve sovrastima di `msg_count` nella sola finestra del crash; mitigato scrivendo su Kafka per ultimo).
  - **Elasticsearch snello:** 1 sola policy ILM delete-only + 1 template su `ais-events-YYYY.MM.DD` (~98% volume) + 5 indici fissi con mapping espliciti; procedura esplicita manuale `make reset-es` (`infra/init-es.sh --reset`).
  - **Modello Silver snello (~30 campi, 100% usati) & 100% metriche DQ preservate:** eliminati 19 campi inutilizzati; preservate tutte le 16 metriche DQ (inclusi tutti i 5 `sentinel_counts.*`, `same_second_events`, `watermark_dropped_msgs`, `exact_duplicate_msgs`) in un solo `.agg()` per batch + `StreamingQueryProgress`, delegando `latency p50/p95` e breakdown `dest_resolution_status` alle aggregazioni native Kibana/ES su `ais-events-*`.
- **Verifiche:**
  - Controllo coerenza cross-file (`AGENTS/PROJECT_SPEC.md`, `AGENTS.md`, `.env.example`, `Makefile`, `README.md`) e struttura repository → **PASS**
  - Verifica conservazione `digitraffic_sample.jsonl` e `AGENTS/REVIEW.md` → **PASS**
  - `uv run pytest` / `uv run ruff check` → **NOT RUN** (nessun codice applicativo ancora implementato)
- **Definition of Done:** Specifica e scheletro consolidati; Fase 0 aperta (da pianificare/implementare al prossimo passo).
- **Prossimo passo:** pianificare e implementare la **Fase 0** (`scripts/probe_sources.py`, `data_static/unlocode_ports.csv`, `docs/source_validation_report.md`).

---

## 2026-11-26 — Revisione architetturale KISS/YAGNI, distinzione doc tecnica/didattica e riallineamento boilerplate

- **Modulo/Fase:** Pre-Fase 0 (Revisione Progettuale & Documentale)
- **Task:** Revisione critica dell'architettura rispetto ai 5 requisiti ufficiali del corso (scenario Big Data, Spark, Kibana, ingestion, integrazione/arricchimento/DQ), aggiornamento di `AGENTS/PROJECT_SPEC.md`, stesura guida didattica `docs/project_overview.md` e riallineamento dei boilerplate del repository.
- **Stato:** completato
- **Attività svolte:**
  - Riscritta [`AGENTS/PROJECT_SPEC.md`](PROJECT_SPEC.md) come Single Source of Truth tecnica per gli agenti: 3 sorgenti reali (Digitraffic MQTT, Digitraffic Vessels REST, UNECE UN/LOCODE CSV senza coordinate), 1 topic Kafka (`ais.locations.raw`), 3 query Spark 3.5 in `append` mode, 5 indici Elasticsearch fissi con mapping espliciti e 1 dashboard Kibana completa.
  - Scritta [`docs/project_overview.md`](../docs/project_overview.md) come guida discorsiva e didattica per lo studente (spiegazione da zero di MQTT, Kafka, Spark Structured Streaming, Elasticsearch, Kibana e Docker Compose, esempi reali, compromessi, nota cloud VM e domande d'esame).
  - Aggiornato [`AGENTS.md`](../AGENTS.md) con il vincolo di codice semplice/lineare/spiegabile all'orale e riferimento a `reference.py`.
  - Riallineati senza anticipare implementazione: `README.md`, `.env.example`, `Makefile`, `docker-compose.yml`, `infra/init-kafka.sh`, `infra/init-es.sh`, `kibana/export.sh`, `kibana/import.sh`, `services/ingestor/requirements.txt`, `services/spark_app/Dockerfile`, `services/spark_app/requirements.txt`, `docs/source_validation_report.md`, `docs/technical_notes.md`.
- **Decisioni tecniche:**
  - **Rimossi definitivamente (YAGNI):** Open-Meteo (meteo/onde/`sea_state_class`), distanza Haversine e parsing coordinate DMS in UN/LOCODE, 2° topic Kafka (`ais.locations.enriched`), refresh orario/cache locale di `vessels`, indice `ais-vessels-latest` + script Painless, policy ILM + index template giornalieri, e le 3 dashboard separate.
  - **1 solo topic Kafka + 3 query Spark 3.5 in `append` mode:** sfrutta `SPARK-42376` (watermark propagation) e `SPARK-42931` (`dropDuplicatesWithinWatermark`) mantenendo sempre attivo `checkCorrectness` e la deduplicazione su tutte le query; gate sperimentale reale su Spark 3.5.5 previsto già in Fase 2.
  - **Sorgenti di riferimento (`reference.py`):** caricate 1 volta all'avvio di Spark (`unlocode_ports.csv` 4 colonne + 1 GET HTTP a `/api/ais/v1/vessels` con timeout e fallback a DataFrame vuoto).
  - **Risoluzione destinazioni semplificata:** 3 stati conservativi (`EMPTY`, `RESOLVED`, `UNRESOLVED`), Precision > Recall.
  - **Elasticsearch & Kibana:** 5 indici fissi (`ais-events`, `ais-traffic-agg`, `ais-geo-agg`, `ais-dq-metrics`, `ais-quarantine`), pulizia manuale via `make reset-es`; mappa live tramite layer nativo Kibana Maps *Top hits per entity* su `ais-events`; conteggi DQ additivi e listener filtrato solo sulla query `events` (zero doppi conteggi); `watermark_time = received_at` sui record invalidi per garantire la quarantena.
  - **Riferimenti sperimentali:** budget RAM, soglia watermark e moltiplicatori benchmark trattati come valori iniziali da calibrare sui dati reali.
- **Verifiche:**
  - Verifica assenza riferimenti obsoleti sui file operativi e controllo coerenza `git status` / `git diff` → **PASS** (0 residui nei boilerplate; in `PROJECT_SPEC.md` solo divieti espliciti di fuori scope)
  - `uv run pytest` / `uv run ruff check` → **NOT RUN** (nessun codice applicativo ancora implementato).
- **Definition of Done:** Revisione architetturale e documentale completata; Fase 0 pronta per pianificazione e implementazione.
- **Prossimo passo:** pianificare e implementare la **Fase 0** (`scripts/probe_sources.py`, `data_static/unlocode_ports.csv`, `docs/source_validation_report.md`).

---

## 2026-11-26 — Fase 0, Task 0.1: `PLAN.md`, dipendenze locali e `scripts/probe_sources.py`

- **Modulo/Fase:** Fase 0 — Validazione Sperimentale delle 3 Sorgenti
- **Task:** Task 0.1 — Salvataggio piano in `PLAN.md`, dipendenze in `pyproject.toml` e implementazione di `scripts/probe_sources.py`
- **Stato:** completato
- **Attività svolte:**
  - Salvato il piano approvato integralmente in `PLAN.md`.
  - Aggiunte le dipendenze `paho-mqtt>=2.0` e `requests>=2.32` in `pyproject.toml` e sincronizzato l'ambiente con `uv sync` (`uv.lock` generato).
  - Implementato `scripts/probe_sources.py` come script procedurale lineare (senza classi né dipendenze extra) con CLI minimale (`--mqtt-seconds`, `--mqtt-file`, `--unlocode-zip`) per:
    1. Cattura live MQTT WSS Digitraffic (`vessels-v2/+/location`) o lettura JSONL locale (`digitraffic_sample.jsonl` con filtro topic `/location`) e calcolo metriche volume, `msg.retain`, latenza, out-of-order globale/per-MMSI, duplicati esatti (`dedup_hash`), collisioni sub-secondo, sentinelle AIS, `nav_stat_mismatch` e distribuzione geografica preliminare `sea_area`.
    2. Chiamata REST `GET /api/ais/v1/vessels` (`gzip` + `Digitraffic-User`) con verifica schema, unicità MMSI e `%` match sul campione MQTT.
    3. Download/parsing ZIP UNECE UN/LOCODE (`cp1252`, filtro porti marittimi `Function` contenente `'1'`, calcolo `is_ambiguous_name` nello scope baltico/nord-europeo, export CSV 4 colonne senza coordinate `data_static/unlocode_ports.csv`) e valutazione risoluzione destinazioni a 3 stati (`EMPTY`, `RESOLVED`, `UNRESOLVED`).
- **Decisioni tecniche:**
  - Unico file procedurale con funzioni pure separabili per test rapidi senza mock di rete (Task 0.2).
  - Discovery automatica del link `*csv.zip` dalla pagina ufficiale UNECE con fallback a URL diretto noto e opzione `--unlocode-zip` locale.
- **Verifiche:**
  - `uv sync` → **PASS** (`Resolved 14 packages in 713ms`)
  - `uv run ruff check scripts/probe_sources.py` → **PASS** (`All checks passed!`)
  - `uv run python scripts/probe_sources.py --help` → **PASS** (exit 0)
  - `uv run pytest` → **NOT RUN** (test unitari previsti nel Task 0.2)
- **Definition of Done:** tutti e 3 i criteri DoD di Fase 0 ancora aperti (chiusura prevista al Task 0.3 dopo cattura live ≥ 3 min).
- **Blocker:** nessuno.
- **Prossimo passo:** Task 0.2 — Scrittura test unitari essenziali in `tests/test_probe_sources.py` e verifica offline su `digitraffic_sample.jsonl`.

---

## 2026-11-26 — Fase 0 (Chiusura Modulo): Snellimento `probe_sources.py`, cattura live 3 min, `unlocode_ports.csv` e report

- **Modulo/Fase:** Fase 0 — Validazione Sperimentale delle 3 Sorgenti
- **Task:** Snellimento Ponytail di `scripts/probe_sources.py`, esecuzione live (180s), generazione `data_static/unlocode_ports.csv`, compilazione `docs/source_validation_report.md` e chiusura DoD Fase 0.
- **Stato:** completato
- **Attività svolte:**
  - Snellito `scripts/probe_sources.py` da 837 a 410 righe (eliminato boilerplate `print_summary` → `json.dumps`, rimossi helper/rami YAGNI, corretti 2 bug in `on_connect` e check `isinstance(p, dict)`).
  - Sostituito download ZIP UNECE (HTTP 403 da `service.unece.org` / `unece.org`) con il mirror CSV aperto `datasets/un-locode` (`https://raw.githubusercontent.com/datasets/un-locode/main/data/code-list.csv`, HTTP 200, UTF-8) con opzione `--unlocode-csv` locale e fail-fast HTTP prima della cattura MQTT.
  - Eseguita cattura live di `180.2s` sulle 3 sorgenti reali: `6.131` eventi MQTT (`34.03 msg/s`, `571` MMSI, latenza `p50=0.598s` / `p99=2.265s` / `max=4.200s`, `14` collisioni sub-secondo, tutte le 5 sentinelle AIS presenti, `79.03%` `nav_stat_mismatch` sulle navi ferme), `806` navi REST (`98.77%` match MMSI / `99.53%` eventi).
  - Generato e validato `data_static/unlocode_ports.csv` (`17.571` porti marittimi, 4 colonne esatte senza coordinate, `5.291` porti baltici / `76` nomi ambigui).
  - Compilato `docs/source_validation_report.md` con tutti i numeri reali e le calibrazioni per Fasi 2–3 (`SPARK_WATERMARK_DELAY = 1 minute`, box `sea_area` estese a Åland + `Finnish Inland Waters` / Saimaa, regola Spark SQL per nomi bilingue `PRIMARIO (ALTERNATIVO)` e punteggiatura UN/LOCODE).
  - Spuntati i 3 checkbox DoD di Fase 0 in `AGENTS/PROJECT_SPEC.md` e aggiornato `PLAN.md` (mantenuto su disco finché non versionato su Git).
- **Decisioni tecniche:**
  - **Ponytail Ultra su Task 0.2:** evitato `tests/test_probe_sources.py` (non richiesto da `PROJECT_SPEC.md`), sostituito da self-check offline su `digitraffic_sample.jsonl` e verifica reale end-to-end sulle 3 sorgenti.
  - **Mirror CSV UN/LOCODE:** `unece.org` blocca richieste scriptate (HTTP 403); `datasets/un-locode` fornisce gli stessi dati ufficiali UNECE in un singolo CSV UTF-8, semplificando anche il parser.
  - **Nomi bilingue UN/LOCODE:** scoperto sperimentalmente che `FIHEL = "HELSINGFORS (HELSINKI)"`, `FITKU = "TURKU (ABO)"`, ecc.; documentata la regola di espansione bilingue in puro Spark SQL per la Fase 2 senza modificare lo schema a 4 colonne del CSV.
- **Verifiche:**
  - `uv run ruff check .` → **PASS** (`All checks passed!`)
  - Self-check offline (`digitraffic_sample.jsonl` + parser/resolver in memoria) → **PASS** (`990` eventi validi, `328` MMSI)
  - `uv run scripts/probe_sources.py --mqtt-seconds 180` → **PASS** (exit 0, tutte e 3 le sorgenti misurate live)
  - Controllo integrità `data_static/unlocode_ports.csv` (`head`, `wc -l`, `grep` su `FIHEL/FITKU/FIHKO/FIMHQ/FIOUL/SESTO/EETLL/RUULU/RUPRI`) → **PASS** (`17.572` righe inclusa intestazione, 4 colonne esatte)
  - `uv run pytest` → **NOT RUN** (nessun file `test_*.py` previsto fino a Fase 2; esce con codice 5 "no tests collected")
- **Definition of Done:** Fase 0 chiusa al **100%** (3/3 criteri DoD spuntati in `AGENTS/PROJECT_SPEC.md`).
- **Blocker:** nessuno per Fase 0. Nota per Fase 1: verificare disponibilità runtime Docker (`orbstack`/`colima`/Docker Desktop) prima di `docker compose up -d`.
- **Prossimo passo:** committare la Fase 0 (e poi rimuovere `PLAN.md` una volta in cronologia Git) e pianificare la **Fase 1** (Infrastruttura Docker Compose & Ingestion Live).
