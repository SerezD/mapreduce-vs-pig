# Confronto di Prestazioni tra Pig e MapReduce

L'obiettivo di questo progetto è mostrare come, a parità di risultato, una *query* in *pig* sia meno performante di una sua versione progettata in *mapreduce*. 

Sebbene le progettazioni di algoritmi in *mapreduce* richiedano sicuramente un maggiore sforzo implementativo, esse possono comunque risultare un buon alleato nella risoluzione di certe *queries* su *dataset* complessi.

Nello svolgimento, è stato utilizzato un sottoinsieme del *dataset* *yelp*, scaricabile sia da https://www.yelp.com/dataset che da https://www.kaggle.com/yelp-dataset/yelp-dataset.

In particolare, sono state estratte le prime *10000* righe della tabella *user.json*, le prime *5000* di *business.json* e le prime *20000* di *review.json*.

## Dipendenze Generali

Il codice *mapreduce* è stato compilato utilizzando *JavaSE-1.7*; sono inoltre necessarie le corrette librerie di *hadoop* (https://hadoop.apache.org/) e quelle per la lettura di *file json* (https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple/1.1.1).

## Struttura del Progetto

Per visualizzare il codice sorgente sviluppato basta vedere la cartella *"code"*, dove sono presenti anche le *queries* di *pig*.

In *"jars"* sono presenti invece gli eseguibili per il codice *map reduce*.

Infine, la cartella *"images"* contiene semplicemente le immagini utilizzate in questo docuento.

## Query: precisione nelle recensioni degli utenti

La prima *query* vuole vedere quale sia l'accuratezza di ogni utente nel fornire le recensioni. Per questo deve calcolare le differenze tra le recensioni delle attività e la recensione assegnata, e farne una media.

Ad esempio, se un utente ha visitato due attività con recensione media di *4.5* e *4.8* e lui ha assegnato *3* stelle ad ognuno, il risultato sarà *(4.5-3) + (4.8-3) / 2*.

### Codice Pig

Per svolgere la *query* in *pig*, è necessario caricare le due tabelle *review.json* e *business.json* ed estrarne le colonne interessanti, ossia *user_id, business_id* e *stars* per *review*, e *business_id* e *stars* per *business*.

Dopodiché è necessario eseguire il *Join* sull'attributo *business_id* (in modo da ottenere tutte le attività per cui c'è almeno una recensione utente), e calcolare la differenza tra i due campi *stars* (il risultato sarà la differenza tra la recensione media del *business* e la singola recensione di un utente per quel *business*).

Infine, si raggruppano i risultati per ogni utente e se ne esegue la media.

Il comando **ILLUSTRATE** sulla *query* ha prodotto il seguente risultato:

![illustrate_q1](/images/Users_Accuracy/illustrate.png)

Il codice tradotto in *mapreduce* consiste nell'esecuzione di due *jobs* distinti. Nella prima fase di *map* si caricano le tabelle e si eseguono le proiezioni, arrivando al *join* nella fase di *reduce*, che calcola le singole differenze.

Il secondo *job* raccoglie semplicemente i risultati ed esegue la media su ogni utente, facendo uso anche di un *combiner*.

Di seguito è visibile il *mr plan* per i due *job*:

![mrplan1_q1](/images/Users_Accuracy/mr_plan_1.png)

![mrplan2_q1](/images/Users_Accuracy/mr_plan_2.png)

### Codice Map Reduce

La versione implementata direttamente in *mapreduce* si basa sul flusso di esecuzione ottenuto dal *mr plan* precedente.

Gli argomenti da fornire sono in questo caso i percorsi per le due tabelle di input (nell'ordine, *business.json* e *review.json*) e i percorsi per i due output (quello del primo *job* è temporaneo e viene eliminato in automatico, mentre quello del secondo *job* contiene il risultato finale).

#### Job 1: Map

Per questa fase vengono utilizzati due *mappers* distinti. Il primo legge la tabella *business* e il secondo la tabella *reviews*.

Nel primo caso vengono estratti il *business_id* e le *stars* (recensione media del *business*). Nel secondo caso invece vengono estratti: *business_id*, *user_id*, *stars* (recensione dell'utente per questo *business*).

Al *reducer* viene passato come chiave il *business_id*, mentre come valore un'apposita struttura dati (*ValuePair*), che contiene due valori: un *Text* e un *FloatWritable*.

Il *Text* è vuoto se passato dal *business_mapper*, mentre contiene l'*user_id* se passato dal *review_mapper*.

Il *FloatWritable* contiene in ogni caso l'attributo *stars* (che ha però significato diverso in base al caso, può indicare la recensione media di un *business* o la recensione di un utente per un *business*).

#### Job 1: Reduce

Questo è il passo cruciale dell'algortimo: vengono unite le due tabelle e calcolate le differenze tra le recensioni.

In particolare, l'input è un *business_id* (chiave) con associati una serie di *Value Pair*. 

Quest'ultima serie può essere in varie forme (indichiamo con **B** un *Value Pair* mandato dal *business_mapper* e con **R** uno mandato dal *review_mapper*):
- B: un solo *Business Value Pair*
- B [R]: un *Business Value Pair* + uno o più *Review Value Pair*
- [R]: uno o più *Review Value Pair*

Il risultato corretto del *join* è quello che unisce un **B** con i suoi **R** associati. Notiamo che non ci possono essere più **B** associati allo stesso *business_id*, che è l'attributo chiave della tabella *business*. 

Di conseguenza, verranno iterati i *Value Pairs* e si salveranno i *Review Value Pairs* in una lista, mentre incontrando un *Business Value Pair* si salverà il suo valore *stars* in una variabile apposita.

Infine, solo nel caso in cui si sia incontrato il *Business Value Pair*, si procede ad eseguire un ulteriore ciclo sulla lista dei *Review Value Pair*,  scrivendo in output la coppia (*user_id, difference*); dove il valore è ottenuto facendo (*business_stars - review_stars*).

Si noti che se si è nel caso in cui è presente solo un **B**, la lista dei *Review Value Pair* risulterà vuota (non servono ulteriori controlli su questa condizione).

#### Job 2: Map

Questo semplice *mapper* legge i valori temporanei salvati nella forma: *(user_id, difference)* e li passa al *reducer*.

#### Job 2: Reduce

Infine, viene fatta la media sui valori *difference* associati a ciascun *user_id*. 

Il *Group By* in questo caso è implicito nel passaggio tra *Mapper* e *Reducer*.

### Confronto delle Prestazioni

Di seguito si riporta il confronto di tempistiche tra i job *pig* e *mapreduce*.

![meta1_q1](/images/Users_Accuracy/Job1_Metadata.png)

![meta2_q1](/images/Users_Accuracy/Job2_Metadata.png)

Come si può notare, i *job* implementati direttamente in *mapreduce* risultano leggermente più veloci.

Di seguito si riportano per completezza i contatori, in particolare quelli dei *File System*, dei *job* e dei *tasks*.

![FS_q1](/images/Users_Accuracy/FileSystem_Counters.png)

La prima cosa che si nota dai *FS counters* è che in generale i *job mr* sembrano leggere e scrivere meno *bytes*, nonostante un maggior numero di operazioni di lettura per l' *HDFS*.

![Job_q1](/images/Users_Accuracy/Job_Counters.png)

![Task1_q1](/images/Users_Accuracy/Task_Counters_Job1.png)

![Task2_q1](/images/Users_Accuracy/Task_Counters_Job2.png)

Nei *Job Counters* si può notare come il numero di *mappers* e *reducers* lanciati sia lo stesso, sebbene nei *job MR* il tempo di esecuzione totale per le due fasi sia minore (confrontare in particolare il contatore *Millis Maps* per il primo *job*).

Nei *Task Counters* vediamo che il numero dei *records* letti e scritti è lo stesso, a conferma che le due versioni operano lo stesso algoritmo.

Inoltre, notiamo come i *job MR* richiedano un minor tempo di computazione da parte della *CPU*.

In generale dunque, i *job mr* risultano più ottimizzati delle implementazioni in *pig*, sia come tempistiche che come numero totale di *bytes* letti e scritti. 

## Query: recensioni medie per le attività di Las Vegas

Per tutte le attività presenti a *Las Vegas*, trovare la media delle recensioni utente su quelle attività, suddivise per anno.

Ad esempio, se a *Las Vegas* c'è un'attività *A* aperta nel *2018*, si dovrà trovare la recensione media degli utenti per l'attività *A* nell'anno *2018*, *2019* e *2020*. 

### Codice Pig

Il codice (sia di *pig* che di *mr*), è abbastanza simile a quello della *query* precedente.

Come prima cosa, si caricano le tabelle *business* e *review* e si estraggono i campi e le righe utili.

Per *business* si tiene solo l' *id* e si estraggono le righe in cui la città è *Las Vegas*. Per *review* si estraggono il *business_id*, la recensione (*stars*) e il campo *year*; ricavato come sottostringa dell'attributo *date*.

In seguito è necessario computare il *Join* tra le due tabelle, per ottenere tutte le recensioni fatte sui *business* di *Las Vegas*.

Infine, si esegue il *Group By* su *year* e *business_id* e la media sull' attributo *stars* per ogni gruppo.

Il comando **ILLUSTRATE**  ha prodotto il seguente risultato:

![illustrate_q2](/images/Review_By_Year/illustrate.png)

Come nella precedente *query*, anche in questo caso il codice *mapreduce* prodotto è diviso in due *job* distinti. 

Nel primo *mapper* si estraggono i campi con proiezioni e selezioni e si esegue il *join*, mentre il corrispondente *reducer* semplicemente esegue un'ulteriore proiezione.

Il secondo *job* si occupa del *group by* e della computazione della *media*, facendo uso anche di un *combiner*.

Di seguito è visibile il *mr plan* per i due *job*:

![mrplan1_q2](/images/Review_By_Year/mr_plan1.png)

![mrplan2_q2](/images/Review_By_Year/mr_plan2.png)

### Codice Map Reduce

La versione implementata direttamente in *mapreduce* si basa sul flusso di esecuzione ottenuto dal *mr plan* precedente.

Gli argomenti da fornire sono in questo caso i percorsi per le due tabelle di input (nell'ordine, *business.json* e *review.json*) e i percorsi per i due output (quello del primo *job* è temporaneo e viene eliminato in automatico, mentre quello del secondo *job* contiene il risultato finale).

#### Job 1: Map

Per questa fase vengono utilizzati due *mappers* distinti. Il primo legge la tabella *business* e il secondo la tabella *reviews*.
Entrambi forniscono al *reducer* una chiave *Text* che rappresenta il *business_id*, e un valore *Value Pair* definito come sopra.

In particolare, il *Business Mapper* estrae i campi *business_id* e *city*, e manda al *reducer* solo i *records* in cui la città è *Las Vegas*. In questo caso il *Value Pair* corrisponde sempre ai valori *("Las Vegas", 0)*.

Il *Review Mapper* deve estrarre il *business_id*, la recensione (*stars*) e l'anno (come sottostringa del campo *date*). Dopodiché invia queste informazioni al *reducer*, scrivendo nel *Value Pair* l'anno (come *Text*) e la recensione (come *FloatWritable*).


#### Job 1: Reduce

Similmente a prima, il *reducer* itera una prima volta gli inputs per distinguere tra *Business Value Pairs* e *Review Value Pairs*.

In questa prima fase, si occupa di controllare che il *Business Mapper* abbia effettivamente inviato il proprio *Value Pair* (segno che l'attività considerata ha sede a *Las Vegas*), e salva inoltre i valori riguardanti le recensioni in un'apposita struttura dati.

Nella seconda fase (che avviene solo nel caso in cui sia stato decretato che il *business* è a *Las Vegas*), si iterano le recensioni per scriverle come risultato intermedio, usando come chiave il *business_id* e come valore il solito *Value Pair: ("year", stars)*.

#### Job 2: Map

Il secondo *mapper* legge i valori temporanei che saranno composti da tre campi in ogni riga, e li manda al *reducer* computando di fatto il *group by*. 

In questa fase ci si appoggia alla struttura dati *Text Pair*, contenente due campi di tipo *Text*.

I valori che arrivano al *reducer* sono una chiave *Text Pair: ("year", "business_id")* e un valore *FloatWritable: stars*.

#### Job 2: Reduce

L'ultimo *reducer* riceve i gruppi: *("year", "business_id")* e per ognuno computa la media sulle recensioni ottenute.

### Confronto delle Prestazioni

Di seguito si riporta il confronto di tempistiche tra i job *pig* e *mapreduce*, eseguiti sullo stesso sottoinsieme del *dataset*.

![meta1_q2](/images/Review_By_Year/Job1_Metadata.png)

![meta2_q2](/images/Review_By_Year/Job2_Metadata.png)

Anche in questo caso, i *job* di *mapreduce* risultano leggermente più veloci nella computazione.

Di seguito si riportano i contatori, in particolare quelli dei *File System*, dei *Jobs* e dei *Tasks*.

![FS_q2](/images/Review_By_Year/FileSystem_Counters.png)

Nei contatori del *File System* vediamo che i *Mappers* dei *mr job* eseguono più operazioni in lettura (per l'*hdfs*) ma che in generale leggono e scrivono meno *bytes* delle contromparti in *pig* (sebbene i risultati siano gli stessi).

![Job_q2](/images/Review_By_Year/Job_Counters.png)

Nei *job counters* si nota che le due versioni lanciano lo stesso numero di fasi *map* e *reduce*, ma che in generale le tempistiche dei *mr jobs* sono inferiori.

![Task1_q2](/images/Review_By_Year/Task_Counters_Job1.png)

![Task2_q2](/images/Review_By_Year/Task_Counters_Job2.png)

La prima cosa che si nota sui *Task Counters* è che il secondo *pig job* ha fatto uso del *combiner*.

Nonostante questo, in entrambi i *job* (a partità di *record* letti e scritti da *mappers* e *reducers*), si nota un minor tempo di computazione da parte dei *job mr*.

Anche in questo caso dunque, i *job mr* risultano più ottimizzati delle implementazioni in *pig*, sia come tempistiche che come numero totale di *bytes* letti e scritti. 

## Query: numero medio di recensioni fornite dagli utenti

Data una certa attività, fornire il numero medio di recensioni degli utenti che ne hanno scritta 
una per quel business.

Ad esempio, se due utenti A e B hanno scritto entrambi una recensione per l'attività x considerata,
e in totale A ha scritto 50 recensioni, mentre B solo 30; la media calcolata sarà 40 = (50 + 30)/2.

Come attività è stata scelta quella con *id = d4qwVw4PcN-_2mK2o1Ro1g*, in quanto nella tabella *review_small* utilizzata presenta un buon numero di recensioni (36).

### Codice Pig

Il codice *pig* carica le due tabelle *review* e *user*. 

Dalla prima mantiene solo le righe che riguardano il *business* desiderato e proietta sugli utenti (*user_id*), facendone anche il *DISTINCT*.

In questo modo si ottengono tutti gli utenti che hanno fornito una recensione per il *business* scelto, senza avere duplicati (un utente potrebbe aver fornito più recensioni su quel *business*).

Della tabella *user* invece si proiettano le colonne *user_id* e *review_count*, che contiene il numero di recensioni fatte dall'utente.

A questo punto si esegue il *Join*, tenendo solo i *review_count*. Infine, esegue la media di questi valori per ottenere il risultato finale.

Il comando **ILLUSTRATE**  ha prodotto il seguente risultato:

![illustrate_q3](/images/Avg_Rev_Number/illustrate.png)

In questo caso *pig* ha tradotto il codice dividendo il carico in tre diversi *jobs*. I primi due hanno il compito di caricare le tabelle ed eseguire il *Join*, mentre il terzo computa la media finale sui *counts* ottenuti, sempre facendo uso di un *combiner*.

![mrplan_q3](/images/Avg_Rev_Number/mr_plan.png)

### Codice Map Reduce

In questa *query* si è provato a suddividere il carico di lavoro in soli due *jobs*, diversamente da quanto fatto da *pig*.

Anche in questo caso è dunque necessario fornire come argomenti i percorsi per le due tabelle di input (nell'ordine, *user.json* e *review.json*) e i percorsi per i due output (quello del primo *job* è temporaneo e viene eliminato in automatico, mentre quello del secondo *job* contiene il risultato finale).

#### Job 1: Map

Per questa fase vengono utilizzati due *mappers* distinti. Il primo legge la tabella *user* e il secondo la tabella *review*.

Al *reducer* si fornisce come chiave un *Text: user_id* mentre come valore un *IntWritable*, che conterrà il *review_count* dell'utente per la tabella *user*, mentre il valore costante *-1* per la tabella *review*.

#### Job 1: Reduce

Il *reducer* deve operare il *join* tra le tabelle. Osserviamo che per ogni *user_id* arriveranno:

- 0 o 1 *review count* dalla tabella *user*, indicante che questo utente è presente o no nel sottoinsieme *user_small* (teoricamente sull'intero *dataset* tutti gli utenti che hanno fornito una recensione dovrebbero essere anche registrati come utenti nella tabella *user*).

- 0 o più valori "*-1*", indicanti che questo utente ha fornito 0 o più recensioni per il *business* selezionato.

Sarà dunque necessario iterare su questi valori per verificare che entrambe le tabelle abbiano fornito almeno un valore, e in caso affermativo registrare il *review_count* dell'utente.

Si scriverà quindi in *output* come chiave un *Text* vuoto e come valore il *review_count*, cioè un *IntWritable*.

#### Job 2: Map

Il secondo *mapper* legge i *review_count* e li passa nello stesso formato al *reducer*

#### Job 2: Reduce

L'ultimo *reducer* riceve i valori in un unico gruppo, in quanto c'è una sola chiave di tipo *Text* (la stringa vuota). 

Computa pertanto la media e la scrive come risultato finale.

### Confronto delle Prestazioni

In questo caso non c'è un confronto diretto tra i *job* di *pig* e quelli di *mr*.

![time_q3](/images/Avg_Rev_Number/time.png)

Notiamo tuttavia che c'è un netto vantaggio di tempo nell'esecuzione di *map reduce*, visto il *job* in meno.

Il *job mr 2* e il *job pig 3* hanno impiegato entrambi circa *14* secondi per essere eseguiti, in quanto entrambi implementano le stesse operazioni (*group by* e *avg*). 

Potrebbe essere pertanto interessante farne un confronto diretto sui contatori.

![fs_q3](/images/Avg_Rev_Number/fs_counters.png)

Per quanto riguarda i contatori del *File System* notiamo che alla fine vengono scritti per *hdfs* circa lo stesso numero di *bytes*, che sono quelli corrisponenti alla media finale.

![job_q3](/images/Avg_Rev_Number/job_counters.png)

Nei *job counters* notiamo che il tempo di esecuzione è molto simile, e diversamente dalle precedenti *queries* in questo caso il *reducer mr* sembra essere leggermente più lento di quello di *pig*.

![task_q3](/images/Avg_Rev_Number/task_counters.png)

I contatori dei *task* spiegano perchè la fase di *reduce* in questo caso è più veloce per *pig*: vediamo infatti che l'uso del *combiner* permette al *reducer* di svolgere meno lavoro, in quanto arriva in input un solo record (contro i sette di *mr*). 

Se si prova ad implementare una versione in *mr* che fa uso del *combiner* vediamo infatti che i *counters* risultano più simili a quelli di *pig*.

![job_comb_q3](/images/Avg_Rev_Number/job_combiner.png)

![task_comb_q3](/images/Avg_Rev_Number/task_combiner.png)

Il tempo di esecuzione dei *reducer* rimane comunque molto simile, ma l'implementazione del *combiner* poitrebbe essere decisiva in altre situazioni, dove i record da esaminare sono di più.