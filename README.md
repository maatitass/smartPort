# Smart Port

Progetto universitario per il corso **LSDM – Big Data Processing**: pipeline Big Data locale in **Docker Compose** per acquisire in streaming, validare, deduplicare, integrare, aggregare con **Apache Spark** e visualizzare in **Kibana** il traffico marittimo AIS nell'area finlandese/baltica.

- **Specifica tecnica operativa (Ground Truth):** [`AGENTS/PROJECT_SPEC.md`](AGENTS/PROJECT_SPEC.md)
- **Guida didattica all'architettura e alle tecnologie:** [`docs/project_overview.md`](docs/project_overview.md)

## Struttura del progetto

- `scripts/` — Verifica sperimentale delle 3 sorgenti (Fase 0) e replay DQ/benchmark (Fase 5)
- `data_static/` — Catalogo porti marittimi UNECE UN/LOCODE pulito (Fase 0)
- `infra/` — Bootstrap Kafka (`init-kafka.sh`, 1 topic) ed Elasticsearch (`init-es.sh`, 5 indici fissi + `--reset`) (Fase 1)
- `services/ingestor/` — Acquisizione stream MQTT Digitraffic → Kafka `ais.locations.raw` (Fase 1)
- `services/spark_app/` — Caricamento sorgenti di riferimento (`reference.py`) + 3 query Spark Streaming in `append` mode (Fasi 2–3)
- `kibana/` — 1 dashboard Kibana completa esportabile/re-importabile (Fase 4)
- `tests/` — Suite `pytest` essenziale e fixture DQ (Fasi 2, 3, 5)
- `docs/` — Guida didattica (`project_overview.md`), report sorgenti (`source_validation_report.md`) e note tecniche/benchmark (`technical_notes.md`)

## Stato attuale

Revisione architetturale KISS/YAGNI, specifica tecnica e guida didattica completate. Le istruzioni complete di avvio locale, pulizia manuale indici e demo orale verranno completate entro la Fase 5.
