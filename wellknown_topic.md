# Topic

Tutti iniziano con `$` per evitare sovrapposizioni con i topic generici dei broker pre-esistenti.

In altre parole, si suppone di poter avviare Orbitalis in un sistema pre-esistente.

TBD: la separazione può essere http-style con `/` oppure mqtt-style con `.`

> [!IMPORTANT]
> Nella modellazione suppongo che l'invio/ricezione dei messaggi sia sempre affidabile, ma chiaramente questo non è vero (e.g. MQTT con QoS=0).
> Si può sempre pensare di aggiungere ulteriori ACK.



## Connection management

### Handshaking

Tutti i plugins restano sempre in ascolto sul topic `$handshaking.discover`, nel quale i cores inviano messaggi che contengono l'id del core di provenienza e la query con le caratteristiche che deve avere il plugin cercato.
Inoltre, inviano il topic su cui mandare le offerte, ad esempio: `$handshaking.<core-id>.offer`

In `$handshaking.<core-id>.offer` vengono ricevute le risposte, le quali contengono l'id del plugin che si offre di servire il core e il successivo topic su cui mandare la richiesta effettiva (A).
Inoltre, il plugin fornisce una mappa con le associazioni tra nome topic (chiave) e descrizione topic (C).

Al posto di avere i canali di offer, request e ack condivisi, si creano canali (temporanei) dedicati, in modo da ridurre il numero di messaggi da inoltrare a destinatari non necessari.

Potrebbe essere qualcosa del tipo: `$handshaking.<id-core>.<id-pluging>.request` (A) e `$handshaking.<id-core>.<id-pluging>.reject` (B).

Si noti che in questa fase i formati del payload devono essere ben noti (**fissi**) e bisogna assicurare con ragionevole certezza (e.g. uuid4) che i topic siano univoci tra core e plugin.
In questo modo si limita anche il problema di core che si registrano sul plugin impropriamente, ossia senza aver seguito la procedura.

> [!NOTE]
> La ragionevole certezza nell'avere topic univoci per evitare rumore tra le comunicazioni può essere ulteriormente gestita inserendo sempre la sorgente.

In `$handshaking.<id-core>.<id-pluging>.request` (A), il core invia sul topic una mappa che rappresenta i canali gestiti dal core (nella pratica rappresentano i metodi che possono essere utilizzati dal plugin per operare sul core). Inoltre si manda il topic su cui il plugin deve mandare ack, e.g. `$handshaking.<id-core>.<id-pluging>.ack` (D).

In `$handshaking.<id-core>.<id-pluging>.ack` (D) il plugin conferma e la connessione è finalmente aperta.

In `$handshaking.<id-core>.<id-pluging>.reject` (B) può arrivare in qualsiasi momento il rifiuto da core o plugin con la relativa motivazione (ragion per cui ha un topic separato). Motivi di possibile rifiuto:

- Il core ha scelto un altro plugin (race condition)
- Schemi non sono supportati
- Plugin ID in blacklist

> [!NOTE]
> Il check sul core ID viene fatto dal plugin in fase di cernita delle richieste `discover`.

#### Descrizione dei topic (C)

Per semplificare la gestione dei topic, si suppone che ogni topic venga usato come scambio di input e output. Chiaramente chi è in ascolto sa che riceverà gli output, chi invia sa che dovrà mandare gli input.

La descrizione delle tipologie di strutture dati che un topic dovrebbe accettare vengono definite in fase di handshaking. Tali strutture vengono definite tramite appositi formati come ad esempio Avro.

In fase di request e ack vengono inviati gli schemi, supponiamo in formato Avro, da utilizzare.

Questo permette agli orb di avere una **ridotta conoscenza a priori**, infatti devono solo sapere le funzionalità che l'orb a cui ci si connette deve avere, le informazioni sul topic e schema da utilizzare vengono fornite "a run-time".

#### Sicurezza

Per rendere sicure le comunicazioni si potrebbe adottare un meccanismo chiave pubblica/privata da passare in ogni messaggio critico.


#### Esempio

Il core `X` ha bisogno di un plugin, quindi invia `discover`:

```
core (id: X) -- $handshaking.discover --> plugins (tra cui Y e Z)

payload:
- core_id: X
- query: { ... }
- offer_topic: $handshaking.X.offer
```

Rispondono due plugins (`Y` e `Z`):

```
plugin (id: Y) -- $handshaking.X.offer --> core (id: X)

request_topic: $handshaking.X.Y.request
reject_topic: $handshaking.X.Y.reject
functions: {
    topic_name: {
        schema: ...
        description: ...
    }
}
```

```
plugin (id: Z) -- $handshaking.X.offer --> core (id: X)

request_topic: $handshaking.X.Z.request
reject_topic: $handshaking.X.Z.reject
functions: {
    topic_name: {
        schema: ...
        description: ...
    }
}
```


Visto che abbiamo supposto che al core serva solo un plugin, il core manda un reject a `Z` perché il messaggio è arrivato per secondo (evita a `Z` di riservare uno slot fino al timeout):

```
core (id: X) -- $handshaking.X.Z.reject --> plugin (id: Z)

reason: no longer necessary
```

Mentre al plugin `Y` viene mandata la richiesta formale (si suppone che gli schemi inoltrati da `Y` vadano bene):

```
core (id: X) -- $handshaking.X.Y.request --> plugin (id: Y)

ack_topic: $handshaking.X.Y.ack
functions: {
    topic_name: {
        schema: ...
        description: ...
    }
}
```

Se il plugin Y valida correttamente gli schemi manda l'ack al core (altrimenti reject):

```
plugin (id: Y) -- $handshaking.X.Y.ack --> core (id: X)
```


### Closing


#### Graceful

`X` vuole chiudere la connessione con `Y`. Si suppone che la connessione sia stata aperta in precedenza e i nomi dei topic concordati.

```
orb X -- $close-intent.X.Y --> orb Y

timeout: <>


orb Y -- $close-intent-ack.X.Y --> orb X


...
altre operazioni tra Y e X
...


orb Y -- $close.X --> orb X

orb X -- $close.Y --> orb Y


[X e Y chiudono la connessione]
```

> [!NOTE]
> `$close-intent.X.Y` e `$close-intent-ack.X.Y` concordati durante l'handshaking. 


#### Non-graceful

```
orb X -- $close.Y --> orb Y
source_id: X
target_id: Y
```

> [!NOTE]
> `$close.Y` sempre presente (a prescindere da handshaking).

> [!IMPORTANT]
> La protezione da fake close va assicurata con altri meccanismi, e.g. chiave pubblica/privata.



### Check

Per controllare che due orb `X` e `Y` siano funzionanti e *consapevoli* di avere una connessione aperta:

```
orb X -- $check-request.Y --> orb Y
source_id: X
target_id: Y
payload: <test>
check_response_topic: $check-response.X.Y
```

```
orb Y -- $check-response.X.Y --> orb X
payload: <test> 
```

> [!NOTE]
> Topic sempre presente `$check-request.Y`, concordabili in handshaking.
>
> `check_response_topic` viene gestito dinamicamente per consentire di inviare check anche nei casi in cui si è persa la configurazione iniziale.

Se dopo un certo periodo di tempo (configurabile) non si ricevono più messaggi, si utilizzano i `check` per valutare un'eventuale inattività. Se l'inattività perpetua, il collegamento viene dato per perso, con le conseguenze del caso.

Lato core, si possono adottare diverse possibili strategie:

- Ricerca di un altro plugin
- Attesa
- ...

Lato plugin:

- Rendere di nuovo subito disponibile lo slot
- Attesa
- ... 

In caso di attesa si smette di interagire con l'interlocutore e si continua a richiedere check fino a che non si ristabilisce oppure fino a un timeout.
Quando il connection check torna positivo si marca il collegamento come nuovamente attivo, previa verifica che anche per l'altro interlocutore sia valido.

Se una connessione tra orb viene data per persa e un vecchio orb tenta di ricontattare, allora si manda un close non graceful al vecchio orb.


### Modifica schemi

```
orb X -- $edit.X.Y --> orb Y
payload: <changes>
```

```
orb Y -- $edit-result.X.Y --> orb X
ok: true/false
```

> [!NOTE]
> `$edit.X.Y` e `$edit-result.X.Y` concordati durante l'handshaking. 





