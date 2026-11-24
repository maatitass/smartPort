# Smart Port

Sistema Big Data per l'acquisizione, l'integrazione multi-sorgente, il controllo qualità e l'analisi in tempo reale del traffico marittimo AIS nell'area finlandese/baltica (corso LSDM — *Big Data Processing*).

> Documentazione architetturale completa: [`PROJECT_SPEC.md`](PROJECT_SPEC.md)

## Struttura del progetto

- `scripts/` — Verifica sperimentale delle sorgenti (Modulo 0)
- `data_static/` — Dataset statico UNECE UN/LOCODE (Modulo 0)
- `infra/` — Bootstrap Kafka KRaft e Elasticsearch ILM/Templates (Modulo 1)
- `services/ingestor/` — Acquisizione stream MQTT Digitraffic → Kafka Bronze (Modulo 2)
- `services/context_sync/` — Sincronizzazione `vessels`, `ports` e `weather` (Modulo 3)
- `services/spark_app/` — Pipeline Spark Structured Streaming Bronze → Silver → Gold (Modulo 4)
- `kibana/` — Dashboard esportabili ed importabili (Modulo 5)
- `tests/` & `services/replay/` — Test automatici, fixture DQ e benchmark (Modulo 6)
- `docs/` — Report sperimentale, data dictionary, decisioni architetturali e guida deploy cloud (Modulo 7)

## Stato attuale

Scheletro strutturale inizializzato. Le istruzioni complete di avvio e configurazione verranno compilate al completamento dei moduli (Modulo 7).
