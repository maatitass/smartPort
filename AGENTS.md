# Smart Port — AGENTS.md

## 1. Contesto, fonte di verità e scope

**Smart Port è un semplice progetto universitario didattico per il corso LSDM – Big Data Processing, non un sistema enterprise o production-ready.**

Lo studente sta imparando le tecnologie utilizzate e deve essere in grado di comprendere, modificare e spiegare autonomamente il codice durante l'esame orale.

L'obiettivo è realizzare un progetto completo, corretto, funzionante e riproducibile, soddisfacendo tutti i requisiti accademici con la minima complessità ingegneristica ragionevole.

- `AGENTS/PROJECT_SPEC.md` è la **Single Source of Truth** per requisiti, architettura, schemi, contratti e Definition of Done (DoD).
- Rispetta tutti gli invarianti della Sezione 8 della specifica.
- Lavora una fase alla volta in ordine di dipendenza (Fasi 0 → 5).
- Non anticipare fasi future né introdurre funzionalità fuori scope o estensioni opzionali.
- Il deployment cloud è uno sviluppo successivo non bloccante. Non introdurre complessità preventiva per supportarlo.
- Risolvi autonomamente bug, dettagli implementativi e refactoring locali che non alterano la specifica.
- Se una modifica altera requisiti, contratti, invarianti o architettura, fermati, motiva la proposta e chiedi conferma prima di aggiornare la specifica.
- Se individui una complessità inutile nella specifica stessa, proponi una semplificazione motivata, senza modificare autonomamente i requisiti.

La qualità del progetto deve derivare dalla correttezza dei risultati, dalla chiarezza del codice e dalla comprensione dei concetti accademici, non dalla sofisticazione dell'architettura.

## 2. Principi progettuali e semplicità del codice (vincolanti)

I seguenti principi si applicano a tutte le attività: progettazione, pianificazione, implementazione, configurazione, testing, documentazione e refactoring.

### 2.1 Principi fondamentali

- **KISS — Keep It Simple, Stupid:** preferisci sempre la soluzione più semplice che soddisfa correttamente il requisito.
- **YAGNI — You Aren't Gonna Need It:** non implementare funzionalità che potrebbero servire in futuro ma che non sono necessarie oggi.
- **DRY — Don't Repeat Yourself:** evita duplicazioni inutili, senza introdurre astrazioni premature. Una piccola duplicazione esplicita è preferibile a un'astrazione sproporzionata.
- **Semplicità prima dell'ottimizzazione:** non ottimizzare preventivamente problemi che non sono stati osservati o per i quali non esiste un requisito esplicito.
- **Funzionalità native:** sfrutta le funzionalità già disponibili nelle librerie e nei framework, evitando di reinventarle.
- **Comprensibilità:** ogni componente deve poter essere compreso e spiegato chiaramente da uno studente.
- **Riproducibilità:** il progetto deve poter essere eseguito, verificato e dimostrato facilmente.
- **Correttezza:** la semplificazione non deve introdurre errori nei risultati, compromettere la sicurezza essenziale o violare i requisiti accademici.

### 2.2 Stile e comprensibilità del codice

**La comprensibilità del codice ha priorità sull'eleganza ingegneristica, sulla generalizzazione, sull'estensibilità futura e sulle ottimizzazioni non necessarie.**

Preferisci una soluzione semplice e ragionevolmente imperfetta dal punto di vista ingegneristico, purché corretta e funzionante, rispetto a una soluzione sofisticata che lo studente non riuscirebbe a comprendere.

Scrivi il codice pensando a uno studente che sta imparando queste tecnologie, non a un team di software engineer che sviluppa un prodotto aziendale.

- Preferisci codice procedurale, lineare, esplicito e facilmente leggibile.
- Preferisci semplici funzioni alle classi, quando sufficienti.
- Utilizza nomi descrittivi per variabili, funzioni e moduli.
- Utilizza costrutti standard e facilmente riconoscibili del linguaggio.
- Mantieni visibile il flusso di esecuzione.
- Suddividi le operazioni non banali in passaggi intermedi comprensibili.
- Evita espressioni eccessivamente compatte, concatenazioni difficili da leggere, annidamenti profondi e sintassi inutilmente sofisticata.
- Introduci funzioni ausiliarie soltanto quando migliorano concretamente la leggibilità o evitano duplicazioni significative.
- Utilizza le API native delle librerie, senza creare wrapper non necessari.
- Utilizza type hints semplici quando migliorano la comprensione delle interfacce.
- Aggiungi commenti e docstring per spiegare i passaggi non banali e le motivazioni delle scelte, senza commentare operazioni già evidenti.
- Gestisci gli errori concreti e rilevanti, senza cercare di prevedere ogni possibile scenario teorico.

**Non cercare di scrivere il codice più breve possibile: cerca di scrivere il codice più semplice da comprendere.**

Una soluzione di 20 righe esplicite può essere preferibile a una soluzione di 5 righe particolarmente compatte e difficile da interpretare.

Allo stesso modo, se un problema può essere risolto chiaramente in 15 righe standard, non scriverne 60 introducendo classi, wrapper o astrazioni.

### 2.3 Prevenzione dell'overengineering

Salvo necessità concreta derivante dalla specifica o dalle librerie utilizzate, evita:

- Classi non necessarie, classi astratte, ereditarietà e gerarchie di classi.
- Factory, dependency injection e design pattern avanzati.
- Framework personalizzati.
- Decoratori personalizzati e metaprogrammazione.
- Wrapper inutili e livelli di indirezione superflui.
- Funzioni generiche progettate per ipotetici utilizzi futuri.
- Configurazioni eccessivamente parametrizzabili.
- Validazioni difensive sproporzionate.
- Cache sofisticate, concorrenza manuale e protocolli personalizzati di sincronizzazione.
- Meccanismi avanzati di recovery e retry personalizzati quando esistono alternative native adeguate.
- Microservizi, dipendenze e strumenti aggiuntivi non necessari.
- Ottimizzazioni preventive e meccanismi tipici dei sistemi production-ready.

Non introdurre una soluzione soltanto perché rappresenta una buona pratica professionale.

**Test di eliminazione:** prima di introdurre un file, un'astrazione, una dipendenza, un'opzione CLI, un manifest, un checksum, una configurazione o una validazione difensiva, verifica:

1. Quale requisito concreto soddisfa?
2. È realmente necessario per la correttezza, la comprensibilità o la riproducibilità?
3. Esiste una soluzione più semplice?
4. Il progetto funzionerebbe correttamente anche senza?

Se l'elemento non è necessario e non porta un beneficio concreto, non aggiungerlo.

### 2.4 Organizzazione del codice e applicazione di DRY

- Preferisci pochi file coesi, lineari e leggibili, ciascuno con una responsabilità chiara.
- Non creare micro-file per sole costanti, helper banali o funzioni utilizzate una sola volta.
- Non imporre limiti arbitrari al numero di righe, funzioni o file.
- Non accorpare responsabilità differenti soltanto per ridurre il numero di file.
- Non applicare DRY in maniera dogmatica: una piccola duplicazione può essere più comprensibile di un'astrazione generica.
- Estrai una funzione o un modulo condiviso soltanto quando esiste una reale responsabilità comune e l'estrazione migliora concretamente il codice.

La struttura migliore è quella che rende il progetto complessivamente più semplice da comprendere, sviluppare e mantenere.

### 2.5 Semplicità della pianificazione

**Dettagliato ≠ complesso.**

Un piano può descrivere approfonditamente una soluzione implementativa molto semplice.

- Applica KISS, YAGNI e DRY già durante la pianificazione.
- Non pianificare funzionalità, astrazioni, configurazioni, validazioni o meccanismi non necessari.
- Non progettare una soluzione complessa con l'intenzione di semplificare successivamente il codice.
- Utilizza un budget indicativo di 2–4 task per script esplorativi e utility (es. Fase 0) e 3–6 task per fasi applicative (Fasi 1–5).
- Adatta il numero di task alla complessità effettiva del modulo, senza frammentare o accorpare artificialmente il lavoro.
- Se un task appare sovradimensionato rispetto alla DoD, proponi una soluzione più semplice.
- Durante l'implementazione puoi adottare autonomamente una soluzione più semplice rispetto al piano, purché non alteri requisiti, contratti, invarianti, architettura o Definition of Done.

### 2.6 Test proporzionati

Concentra i test sulla logica core del progetto (`reference.py`, `transforms.py`, `spark_app`, Fasi 2–3), con particolare attenzione a validazione, pulizia, deduplicazione, Data Integration e aggregazioni.

Per script esplorativi, utility e infrastruttura (Fasi 0, 1, 4), preferisci verifiche funzionali reali e test specifici per parsing, trasformazioni o calcoli non banali.

- Utilizza fixture semplici e rappresentative dei problemi effettivamente osservati.
- Evita mock complessi di CLI, segnali, filesystem e servizi esterni quando una verifica diretta è sufficiente.
- Non introdurre framework di testing personalizzati, fault injection avanzata o sistemi di benchmarking sproporzionati.
- Non ridurre la copertura delle verifiche obbligatorie previste dalla specifica.

### 2.7 Verifica di semplicità e criterio di arresto

Prima di presentare un piano o dichiarare concluso un task, verifica che:

- Non esistano soluzioni equivalenti significativamente più semplici.
- Non siano state introdotte funzionalità, configurazioni o astrazioni superflue.
- Siano state utilizzate le funzionalità native disponibili.
- Il codice sia facilmente leggibile e il flusso di esecuzione sia evidente.
- Lo studente possa comprendere e spiegare autonomamente la soluzione.
- La complessità introdotta sia giustificata dai requisiti reali del progetto.
- Le verifiche siano proporzionate e sufficienti.

Non creare documenti, report o processi aggiuntivi per effettuare questa verifica.

**Quando una soluzione funziona correttamente, soddisfa la specifica, supera le verifiche pertinenti ed è comprensibile, fermati.**

Non continuare a perfezionarla per renderla più elegante, generica, scalabile, estensibile, robusta o production-ready.

Accetta consapevolmente compromessi ingegneristici ragionevoli, senza compromettere correttezza, sicurezza essenziale e requisiti universitari.

## 3. Workflow operativo

### 3.1 Pianificazione (se e solo se richiesta prima di un modulo)

- Analizza `AGENTS/PROJECT_SPEC.md`, il repository, le dipendenze e la DoD senza modificare codice.
- Individua la soluzione implementativa più semplice che soddisfa integralmente i requisiti della fase.
- Proponi un piano in task atomici, sequenziali e verificabili, rispettando tutti i principi della Sezione 2 e il budget indicativo di task.
- Il piano deve essere **autosufficiente e direttamente eseguibile**, anche da un modello più piccolo in una nuova sessione, senza dipendere dal contesto precedente.

Per ogni task definisci in modo essenziale e preciso:

- Obiettivo, dipendenze e criteri DoD coperti.
- File da creare o modificare.
- Firme, interfacce, schemi e logica concreta, limitatamente ai requisiti della specifica.
- Edge case reali dalla specifica e gestione minima necessaria degli errori.
- Comando di verifica e risultato atteso.
- **Fuori perimetro (`NON fare`):** funzionalità extra, validazioni o astrazioni non necessarie.

Fornisci dettagli sufficienti a evitare ambiguità, senza trasformare il piano in una specifica enterprise.

Risolvi le ambiguità progettuali durante la pianificazione. Se richiedono modifiche sostanziali alla specifica, chiedi conferma all'utente.

Dopo l'approvazione, salva il piano integrale in `PLAN.md`, senza riassumerlo o alterarlo, e includilo tra le modifiche da versionare.

### 3.2 Implementazione

**Prima di iniziare:**

- Leggi la fase interessata e le Sezioni 7–8 di `AGENTS/PROJECT_SPEC.md`, oltre agli eventuali contratti e schemi pertinenti.
- Leggi `PLAN.md`, se presente.
- Consulta le ultime voci pertinenti di `AGENTS/DEVELOPMENT_LOG.md`.
- Esegui `git status --short`.
- Ispeziona i file esistenti e individua eventuali modifiche dell'utente da preservare.

**Durante:**

- Implementa soltanto il task autorizzato, rispettando `PLAN.md` quando presente.
- Scrivi codice semplice, procedurale, lineare, esplicito e comprensibile, applicando rigorosamente tutti i principi della Sezione 2.
- Sfrutta le funzionalità native delle librerie.
- Preserva le modifiche preesistenti dell'utente.
- Aggiungi test proporzionati.
- Non lasciare TODO bloccanti né implementazioni fittizie.
- Non effettuare refactoring estranei al task.
- Non introdurre funzionalità aggiuntive soltanto perché sarebbero facili da implementare in quel momento.

Se individui una soluzione implementativa più semplice, puoi adottarla autonomamente purché rispetti la specifica e i criteri di accettazione.

Se emergono conflitti, blocker, modifiche sostanziali o complessità sproporzionate, fermati e proponi una soluzione motivata prima di procedere.

**A fine task:**

- Esegui le verifiche pertinenti.
- Controlla il diff e verifica che non siano state introdotte complessità superflue.
- Rimuovi eventuali import, helper o configurazioni inutilizzati introdotti durante il task.
- Spunta i checkbox di `PLAN.md` e della DoD in `AGENTS/PROJECT_SPEC.md` soltanto dopo verifica reale.
- Aggiorna la documentazione interessata e `AGENTS/DEVELOPMENT_LOG.md`.
- Proponi un messaggio di commit in inglese secondo Conventional Commits.

Nella risposta finale spiega sinteticamente in italiano:

- Cosa è stato implementato.
- Come funziona il codice.
- Perché sono state adottate le principali scelte tecniche.
- Quali verifiche sono state eseguite e con quale risultato.
- Eventuali limitazioni o problemi residui.
- Il prossimo passo.

Le spiegazioni devono aiutare lo studente a comprendere il codice, senza trasformarsi in trattati teorici o documentazione ridondante.

Non committare né procedere al task successivo senza autorizzazione.

**A fine modulo:**

- Verifica integralmente la DoD.
- Esegui tutte le verifiche complessive previste dalla specifica.
- Registra il riepilogo finale in `AGENTS/DEVELOPMENT_LOG.md`.
- Rimuovi `PLAN.md`, se presente, dopo aver verificato che il piano integrale sia conservato nella cronologia Git.
- Proponi il commit finale e il merge su `main`, senza eseguirli autonomamente.

Non dichiarare completo un modulo con verifiche obbligatorie mancanti o fallite.

## 4. Convenzioni, tooling, documentazione e verifiche

### 4.1 Convenzioni linguistiche

- **Inglese:** codice, nomi dei file, variabili, funzioni, commenti, docstring, log, schemi, test e messaggi di commit.
- **Italiano:** chat, spiegazioni del codice, `README.md`, `PLAN.md`, specifica, registro di sviluppo e documentazione in `docs/`.

### 4.2 Tooling

- Usa `uv` per la gestione dell'ambiente Python locale.
- Usa `requirements.txt` con dipendenze pinnate nei Dockerfile dei servizi Python.
- Usa i target del `Makefile` quando presenti.
- Rispetta i requisiti di compatibilità ARM64/AMD64 definiti nella specifica.
- Considera i budget delle risorse come riferimenti iniziali da validare sperimentalmente, non come vincoli arbitrari che richiedono ottimizzazioni preventive.
- Non introdurre nuovi strumenti o dipendenze se quelli esistenti sono sufficienti.

### 4.3 Verifiche

Prima di chiudere un modulo Python, esegui, quando configurati:

```bash
uv run pytest
uv run ruff check .
```

Esegui inoltre le verifiche end-to-end e i benchmark richiesti dalla DoD.

Isola sempre fixture e benchmark dai dati live, utilizzando topic, indici, checkpoint o ambienti di test separati secondo la soluzione più semplice coerente con la specifica.

**Evidence-First:**

- Non indebolire test o criteri di accettazione per far risultare completato un task.
- Non inventare schemi API, metriche, risultati sperimentali o garanzie tecniche.
- Consulta la documentazione ufficiale quando esistono dubbi tecnici rilevanti.
- Distingui sempre `PASS / FAIL / NOT RUN`.
- Non dichiarare verificato ciò che non è stato effettivamente verificato.
- Non dichiarare completo un modulo con verifiche obbligatorie mancanti o fallite.
- Documenta le limitazioni effettive e i compromessi consapevolmente accettati.

### 4.4 Responsabilità della documentazione

Mantieni una separazione chiara tra i documenti:

- `AGENTS.md`: istruzioni comportamentali e operative per gli agenti.
- `AGENTS/PROJECT_SPEC.md`: specifica tecnica operativa, requisiti, architettura, contratti, roadmap, invarianti e DoD. È destinata principalmente agli agenti.
- `docs/project_overview.md`: guida didattica e discorsiva destinata principalmente allo studente, per comprendere il progetto e preparare l'esame orale.
- `AGENTS/DEVELOPMENT_LOG.md`: storico cronologico dello sviluppo, delle decisioni e delle verifiche.
- `PLAN.md`: piano operativo temporaneo della fase corrente.
- `README.md`: presentazione del progetto, istruzioni di esecuzione e dimostrazione.

La guida didattica deve spiegare le tecnologie, i concetti teorici, il funzionamento della pipeline, le motivazioni delle scelte, le alternative e i compromessi adottati, utilizzando esempi concreti quando utili.

Mantieni la documentazione coerente con la specifica e con il comportamento effettivo del codice.

Non duplicare inutilmente informazioni e non introdurre nuovi documenti senza una responsabilità concreta.

## 5. Sicurezza e Git

Mantieni un livello di sicurezza essenziale e proporzionato al contesto universitario.

La semplicità non giustifica l'esposizione di credenziali, servizi non protetti o cancellazioni accidentali di dati.

- Non esporre o committare segreti.
- Usa `.env` per le configurazioni riservate e mantieni `.env.example` sincronizzato con placeholder.
- Mantieni attiva la security Elasticsearch.
- Pubblica le porte host esclusivamente su `127.0.0.1`, secondo la specifica.
- Non sovrascrivere modifiche preesistenti dell'utente.
- Non introdurre meccanismi avanzati di sicurezza o gestione delle identità non richiesti.

Senza autorizzazione esplicita, non eseguire:

- `git commit`
- `git push`
- `git reset`
- `git stash`
- `git rebase`
- `git clean`
- Merge di branch.
- Cancellazioni ricorsive.
- `docker compose down -v`
- `make reset-es`
- Rimozioni di dati persistenti, topic, indici e checkpoint live.

## 6. Registro di sviluppo (`AGENTS/DEVELOPMENT_LOG.md`)

Consulta il registro all'inizio delle attività, leggendo soltanto le voci pertinenti e verificando sempre lo stato reale del repository.

### Quando aggiornarlo

Aggiornalo soltanto:

- Al termine di un task di `PLAN.md` o alla chiusura di un modulo.
- A seguito di un cambio rilevante della specifica, dell'architettura o della configurazione.
- A metà task, quando richiesto esplicitamente dall'utente.
- Quando un task si interrompe o rimane bloccato.

Non aggiornarlo dopo ogni singolo messaggio, per semplici domande in chat o per piccoli aggiustamenti iterativi.

### Contenuto delle voci

Aggiungi una voce cronologica completa, senza riscrivere lo storico.

Ogni voce deve contenere:

- **Data, modulo, task e stato:** `completato` / `parziale` / `bloccato`.
- **Attività svolte:** file e modifiche principali.
- **Decisioni tecniche:** scelte rilevanti e relative motivazioni.
- **Verifiche:** comandi reali e risultati `PASS / FAIL / NOT RUN`.
- **Definition of Done:** criteri chiusi e aperti.
- **Blocker:** eventuali problemi residui.
- **Prossimo passo:** attività successiva prevista.

Mantieni le voci informative ma essenziali, senza duplicare integralmente il piano, il codice o la documentazione.

Quando aggiorni il registro, confermalo nella risposta finale insieme alla sintesi delle modifiche, delle verifiche e dei problemi residui.

---

## Principio guida finale

**Massimo valore accademico, minima complessità ingegneristica ragionevole.**

Smart Port deve essere un semplice progetto universitario, completo, corretto, funzionante, riproducibile e comprensibile.

Non deve essere perfetto dal punto di vista ingegneristico, anticipare ogni possibile problema futuro o rispettare tutti gli standard di un sistema industriale.

Non scrivere il codice che scriverebbe un senior software engineer per un prodotto aziendale destinato a durare dieci anni.

Scrivi codice universitario semplice, esplicito e didattico, che lo studente possa comprendere interamente, mantenere e spiegare durante l'esame orale.

Questo principio vale tanto per il piano quanto per il codice effettivamente scritto.

**Se funziona correttamente, rispetta la specifica, supera le verifiche ed è facile da comprendere, è sufficiente. Fermati lì.**Pianifica la Fase X seguendo AGENTS.md e AGENTS/PROJECT_SPEC.md. Mantieni il piano minimale e verificabile. /plan /ponytail
(Aggiungi /grill-me solo nelle fasi con scelte aperte, es. Fase 2 Spark o Fase 4 Dashboard.)
Leggi il piano, taglia qualsiasi astrazione inutile e approva per farlo salvare in PLAN.md.