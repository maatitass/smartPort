# Report di Validazione Sperimentale delle Sorgenti (Fase 0)

> Eseguito il **2026-11-26** tramite `uv run scripts/probe_sources.py --mqtt-seconds 180` (cattura live completata alle `16:59:15 UTC`).

---

## 1. Sorgente 1 — Digitraffic AIS Locations (MQTT over WSS)

- **Endpoint / Topic:** `wss://meri.digitraffic.fi:443/mqtt` → `vessels-v2/+/location`
- **Durata cattura live:** `180.2 s` (~3 min)

| Metrica | Valore misurato | Note |
|---|---|---|
| Messaggi totali / validi / invalidi | `6.131` / `6.131` / `0` | 100% conformi al formato JSON/AIS atteso nella finestra |
| Frequenza media | `34.03 msg/s` | Conferma la stima ~35 msg/s (~2,9M msg/giorno) |
| MMSI distinti | `571` | ~571 navi attive simultanee in 3 minuti |
| Messaggi `msg.retain == true` | `0` (`0.00%`) | Il broker non ha inviato retained su `location` in questa sessione; il flag resta comunque gestito in `ingestor.py` e Spark |
| Latenza `received_at - time` | `p50 = 0.598s`, `p95 = 1.311s`, `p99 = 2.265s`, `max = 4.200s` | Ritardi molto contenuti e regolari |
| Eventi out-of-order (globale) | `0` | Nessun riordino osservato nella finestra di 3 min |
| Duplicati esatti (`dedup_hash`) | `0` | — |
| Collisioni sub-secondo (`(mmsi, time)` uguali, cinematica diversa) | `14` (`0.23%`) | Conferma l'invariante #2: deduplicare sempre su `dedup_hash` completo, **mai** sul solo `(mmsi, event_time)` |
| Sentinelle AIS rilevate | `rot==-128`: `820` (`13.37%`), `heading==511`: `815` (`13.29%`), `navStat==15`: `309` (`5.04%`), `cog>=360`: `239` (`3.90%`), `sog>=102.2`: `40` (`0.65%`) | Tutte e 5 le sentinelle AIS sono presenti in modo significativo nei dati reali |
| `nav_stat_mismatch` (`sog < 0.5` e `navStat == 0`) | `2.567` su `3.248` rilevazioni ferme (`79.03%`; `41.87%` del totale) | Conferma la forte inaffidabilità di `navStat` rispetto alla velocità effettiva (`sog`) |
| Copertura geografica | `lat ∈ [58.2329, 65.7691]`, `lon ∈ [17.0347, 29.5680]` | Bacino baltico settentrionale, golfo di Finlandia, Botnia e laghi interni finlandesi |

**Distribuzione preliminare `sea_area` (prima della calibrazione):**
- `Gulf of Finland`: `2.432` (`39.67%`)
- `Archipelago Sea`: `1.506` (`24.56%`)
- `Bothnian Bay`: `754` (`12.30%`)
- `Other`: `709` (`11.56%`)
- `Bothnian Sea`: `612` (`9.98%`)
- `Northern Baltic Proper`: `118` (`1.92%`)

---

## 2. Sorgente 2 — Digitraffic AIS Vessels (REST JSON gzip)

- **Endpoint:** `GET https://meri.digitraffic.fi/api/ais/v1/vessels` (`Accept-Encoding: gzip`, `Digitraffic-User`)
- **Tempo di risposta:** `5.65 s` (compatibile con timeout `10s` in `reference.py`)
- **Dimensione decompressa:** `262.735 byte` (~257 KB, `Content-Encoding: gzip`)
- **Record / MMSI:** `806` record, `806` MMSI distinti (`0` duplicati)
- **Schema reale osservato (14 chiavi):** `callSign`, `destination`, `draught`, `eta`, `imo`, `mmsi`, `name`, `posType`, `referencePointA`, `referencePointB`, `referencePointC`, `referencePointD`, `shipType`, `timestamp`
- **Match MMSI sul campione MQTT live:**
  - `98.77%` sugli MMSI distinti (`~564 / 571`)
  - `99.53%` sugli eventi validi MQTT (`~6.102 / 6.131`)
  - Il residuo ~1.2% di MMSI non presenti conferma la necessità del **Broadcast Left Join** (`metadata_matched = false`, `ship_category = "Unknown"`).

---

## 3. Sorgente 3 — UNECE UN/LOCODE (`data_static/unlocode_ports.csv`)

- **Sorgente scaricata:** mirror open-source `datasets/un-locode` (`https://raw.githubusercontent.com/datasets/un-locode/main/data/code-list.csv`, basato sui dati ufficiali UNECE), adottato perché `unece.org` / `service.unece.org` rispondono HTTP `403` ai client HTTP non interattivi.
- **Catalogo generato:** `data_static/unlocode_ports.csv` — `17.571` porti marittimi (`Function` contenente `'1'`), 4 colonne esatte (`locode,port_name_norm,country,is_ambiguous_name`, **senza coordinate**).
- **Scope baltico / nord-europeo (13 paesi):** `5.291` porti marittimi, di cui `76` con `is_ambiguous_name = true`.

### Distribuzione risoluzione destinazioni su `806` navi reali (regole base)

| Stato | Navi | % |
|---|---:|---:|
| `RESOLVED` | `352` | `43.67%` |
| `UNRESOLVED` | `395` | `49.01%` |
| `EMPTY` | `59` | `7.32%` |

**Top 15 destinazioni `UNRESOLVED` osservate:**
`HELSINKI` (20), `UST LUGA` (9), `TURKU` (8), `HARBOUR ASSISTANCE` (7), `SWEDISH SAR VESSEL` (5), `FAIRWAY WORK` (5), `FOR ORDERS` (5), `FAIRWAY WORKS` (5), `HANKO` (5), `PILOT CH 11` (4), `PRIMORSK` (4), `MARIEHAMN` (4), `UST-LUGA` (4), `OULU` (4), `KAUKAS` (3).

**Scoperta chiave nei dati UN/LOCODE:**
I principali porti bilingue finlandesi e russi compaiono in UN/LOCODE con nome doppio `PRIMARIO (ALTERNATIVO)` o punteggiatura:
- `FIHEL` → `HELSINGFORS (HELSINKI)`
- `FITKU` → `TURKU (ABO)`
- `FIHKO` → `HANGO (HANKO)`
- `FIMHQ` → `MAARIANHAMINA (MARIEHAMN)`
- `FIOUL` → `OULU (ULEABORG)`
- `RUPRI` → `PRIMORSK (KOIVISTO)`
- `RUULU` → `UST'-LUGA`

Con il solo match letterale sull'intera stringa `port_name_norm`, anche `HELSINKI` (20 navi) e `TURKU` (8 navi) finivano in `UNRESOLVED`. Al contrario, testi operativi liberi (`HARBOUR ASSISTANCE`, `SWEDISH SAR VESSEL`, `FAIRWAY WORK`, `FOR ORDERS`, `PILOT CH 11`) sono correttamente classificati `UNRESOLVED`.

---

## 4. Calibrazione Parametri per Fasi 2–3 *(Decisioni Aperte Sez. 7 #1 e #3)*

1. **Soglia Watermark (`SPARK_WATERMARK_DELAY`):**
   - **Confermato `1 minute` (60s):** la latenza massima misurata live è `4.20s` (`p99 = 2.265s`, `out-of-order = 0`). Un watermark di 1 minuto offre un margine >14× sul peggior ritardo osservato, mantenendo bassa la latenza di emissione delle finestre in `append` mode (~3 min con `SPARK_WINDOW_DURATION = 2 minutes`).

2. **Confini `sea_area` calibrati per Spark SQL (`transforms.py`):**
   L'ispezione delle coordinate classificate `Other` (709 msg, `11.56%`) ha rivelato 2 cluster reali: il corridoio Mar delle Åland (`lat 59.5–60.8, lon 18.0–19.5`) e il traffico AIS sui laghi interni finlandesi / Saimaa (`lat 60.8–63.5, lon 23.0–30.5`). Per la Fase 2 si calibrano i box rettangolari in ordine di priorità:

   | Priorità | `sea_area` | `lat` | `lon` | Motivazione calibrazione |
   |---:|---|---|---|---|
   | 1 | `Gulf of Finland` | `59.3 – 60.8` | `22.9 – 30.3` | Copre Helsinki, Tallinn, Kotka, San Pietroburgo |
   | 2 | `Archipelago Sea` | `59.5 – 60.8` | `18.0 – 22.9` | Esteso da `19.5` a `18.0` per coprire Åland / rotta Stoccolma–Turku |
   | 3 | `Bothnian Bay` | `63.5 – 66.0` | `20.0 – 26.0` | Copre Oulu, Kemi, Raahe |
   | 4 | `Bothnian Sea` | `60.8 – 63.5` | `17.0 – 23.0` | Esteso a `23.0` per coprire costa finlandese (Rauma, Pori, Vaasa) |
   | 5 | `Finnish Inland Waters` | `60.8 – 63.5` | `23.0 – 30.5` | Copre navi commerciali nel sistema lacustre Saimaa |
   | 6 | `Gulf of Riga` | `56.9 – 58.6` | `22.5 – 24.8` | Golfo di Riga |
   | 7 | `Northern Baltic Proper` | `57.0 – 59.5` | `17.5 – 22.9` | Bacino baltico centrale settentrionale |
   | 8 | `Other` | resto | resto | Fallback residuo |

3. **Regole di pulizia destinazioni per Fase 2 (`transforms.py`):**
   - **Separatori multi-scalo confermati:** `<->`, `<>`, `->`, `/` → sempre `UNRESOLVED`.
   - **Miglioramento mirato (Precision > Recall intatta, zero modifiche allo schema CSV):** nel join per nome sul bacino baltico in Spark SQL (Fase 2), normalizzare `-` e `'` in spazio ed espandere i nomi bilingue `PRIMARIO (ALTERNATIVO)` di `unlocode_ports.csv` in entrambi gli alias purché univoci nello scope baltico (`is_ambiguous_name == false`). Questo recupera i principali porti reali (`HELSINKI`, `TURKU`, `HANKO`, `MARIEHAMN`, `OULU`, `PRIMORSK`, `UST LUGA` — +54 navi, ~+6.7 pp di copertura) senza indovinare mai stringhe ambigue o operative.
