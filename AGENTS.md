# Smart Port — AGENTS.md

## 1. Fonte di verità e scope

- `PROJECT_SPEC.md` è la **Single Source of Truth** per requisiti, architettura, schemi, contratti e Definition of Done.
- Rispetta tutti gli invarianti architetturali della sezione 7 della specifica.
- Lavora un modulo alla volta, rispettando l'ordine delle dipendenze (0 → 7). Non anticipare moduli futuri.
- Non introdurre funzionalità fuori scope, estensioni opzionali, dipendenze superflue o astrazioni premature.
- Risolvi autonomamente problemi implementativi, bug e refactoring locali.
- Se una modifica altera requisiti, contratti, invarianti o architettura, chiedi conferma prima di procedere e aggiornare la specifica.

## 2. Workflow operativo

### Prima di iniziare

- Leggi la sezione del modulo interessato e la sezione 7 (Invarianti Architetturali) di `PROJECT_SPEC.md`, oltre alle ultime voci pertinenti di `DEVELOPMENT_LOG.md`.
- Esegui `git status --short` e ispeziona codice, configurazioni e test esistenti.
- Identifica i criteri della Definition of Done interessati.

### Pianificazione incrementale (per modulo)

- Prima di iniziare un modulo (o un task multi-step), proponi un piano suddiviso in task atomici, sequenziali e verificabili.
- Dopo l'approvazione, salva il piano in `PLAN.md` (in italiano, tracciato in Git e non escluso da `.gitignore`).
- Implementa un task alla volta e aggiorna `PLAN.md` solo dopo averne verificato realmente il completamento.
- Al termine di ogni task, aggiorna `DEVELOPMENT_LOG.md` e proponi un messaggio di commit (che includa le modifiche e `PLAN.md`).
- Non procedere al task successivo senza autorizzazione, salvo diversa indicazione esplicita dell'utente.
- A modulo completato e Definition of Done verificata, aggiorna i checkbox in `PROJECT_SPEC.md`, registra il riepilogo finale in `DEVELOPMENT_LOG.md` e rimuovi `PLAN.md`.
- Per bugfix isolati o micro-modifiche locali non è richiesto `PLAN.md`.

### Durante il lavoro

- Preferisci modifiche minime, mirate, leggibili e verificabili.
- Riutilizza il codice esistente e preserva le modifiche dell'utente.
- Aggiungi o aggiorna i test pertinenti.
- Non lasciare implementazioni fittizie o TODO necessari al completamento del task.

### Prima di concludere

- Esegui le verifiche pertinenti e ispeziona il diff finale.
- Aggiorna i checkbox della Definition of Done solo dopo aver verificato realmente i criteri.
- Aggiorna la documentazione interessata e `DEVELOPMENT_LOG.md`.

## 3. Convenzioni e tooling

- **Inglese:** codice, nomi di file, variabili, funzioni, commenti, docstring, log, schemi e test.
- **Italiano:** risposte in chat, `README.md`, `PLAN.md`, specifica, registro di sviluppo e documentazione.
- Usa `uv` per comandi Python e dipendenze locali; nei Dockerfile usa `requirements.txt` con versioni pinnate.
- Rispetta le versioni dello stack, la compatibilità ARM64/AMD64 e il budget di risorse definiti nella specifica.
- Utilizza i target esistenti del Makefile, quando disponibili.

## 4. Verifiche ed Evidence-First

- Prima di completare un modulo Python, esegui `uv run pytest` e `uv run ruff check`, quando configurati.
- Esegui i test di integrazione, end-to-end e benchmark richiesti dalla Definition of Done.
- Isola fixture e benchmark dai dati live, inclusi topic, indici e checkpoint.
- Non indebolire test o criteri di accettazione per ottenere esiti positivi.
- Non inventare schemi API, risultati, metriche o garanzie tecniche. In caso di dubbio, consulta la documentazione ufficiale delle versioni utilizzate.
- Distingui verifiche **PASS / FAIL / NOT RUN**.
- Segnala blocker e limitazioni. Non dichiarare completo un modulo con verifiche obbligatorie mancanti o fallite.

## 5. Sicurezza e Git

- Non esporre o committare segreti. Usa `.env` e mantieni sincronizzato `.env.example` con placeholder.
- Mantieni attiva la security Elasticsearch e pubblica le porte host esclusivamente su `127.0.0.1`.
- Non annullare o sovrascrivere modifiche preesistenti dell'utente.
- Senza autorizzazione esplicita, non eseguire commit, push, reset, stash, rebase, clean, ripristini distruttivi, cancellazioni ricorsive, `docker compose down -v` o eliminazioni di dati persistenti, topic, indici e checkpoint live.

## 6. Registro di sviluppo e continuità

Crea nella root `DEVELOPMENT_LOG.md` se non esiste. Consultalo all'inizio e **aggiornalo obbligatoriamente alla fine di ogni task operativo** (modifiche al repository, test, esperimenti o cambi di stato; non per pure domande esplorative in chat), anche se parziale, fallito o bloccato.

Aggiungi una voce cronologica sintetica senza riscrivere lo storico, indicando:

- **Data, modulo, task e stato:** completato / parziale / bloccato.
- **Attività svolte:** modifiche e file principali.
- **Decisioni tecniche:** motivazioni rilevanti ed eventuali variazioni autorizzate.
- **Verifiche:** comandi realmente eseguiti ed esiti (`PASS / FAIL / NOT RUN`).
- **Definition of Done:** criteri soddisfatti e ancora aperti.
- **Blocker e prossimo passo:** informazioni necessarie per riprendere il lavoro.

Il registro documenta lo stato effettivo dello sviluppo, non sostituisce `PROJECT_SPEC.md` e non introduce nuovi requisiti.

Consulta soltanto le voci pertinenti per evitare di caricare contesto inutile. Verifica sempre lo stato reale del repository prima di riprendere il lavoro.

Al termine del task, riporta sinteticamente in chat modifiche, verifiche, problemi residui e conferma dell'aggiornamento del registro.

---

**Principio guida:** correttezza, semplicità, riproducibilità e trasparenza. Nessuna deriva architetturale silenziosa, nessun risultato inventato, nessun completamento non verificato.
