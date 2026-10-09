# Återkörningsdemo (SYNTETISKA data)

Demon visar inkrementell MERGE, raderingar och att återkörning är säker. All data är
**påhittad**: titlar börjar med `[SYNTETISK]`/`[SYNTHETIC]`, identifierare använder domänen
`synthetic.example`, och alla Gold-rader och exporter har `data_scope` =
"SYNTETISK DEMO-DATA ... Inte verklig statistik". Fixtures finns i
`src/diva_lakehouse/demo_data.py` och serveras genom samma OAI-PMH-klient som live-läget.

Demo skriver endast till tabeller `diva_demo_*` och `Files/diva/demo/...`, och demon
**raderar och återskapar** just dessa vid start. Live-data (`diva_*`, `Files/diva/raw` osv.)
berörs inte.

## Batcher

| Batch | Post | Händelse |
|---|---|---|
| A (2 sidor, resumptionToken) | demo-0001 | ny (Uppsala, svenska, 2 unika ämnesord) |
| | demo-0002 | ny (KTH, engelska) |
| | demo-0090 | tidskriftsartikel → bortfiltrerad (`is_selected=false`) |
| | demo-0003 | ny, publishers från två lärosäten → "Flera lärosäten (ej entydigt)" |
| | demo-0004 | ny, saknar abstract och publisher → "Okänt lärosäte" |
| | (ingen id) | trasig post → karantän |
| B (1 sida) | demo-0002 | ändrad (nytt abstract, nyare datestamp) |
| | demo-0005 | ny |
| | demo-0003 | oförändrat innehåll, nyare datestamp |
| | demo-0004 | deleted header |

## Steg

1. Kör A (`run_id = demo-A`).
2. Kör B (`run_id = demo-B`).
3. Kör B igen med nytt `run_id = demo-B-repeat` (nya Bronze-observationer, inga Silver-ändringar).
4. Bearbeta `run_id = demo-B` igen (samma run_id: rådata återanvänds, inga nya Bronze-rader).

Verifiering (sparas i `Files/diva/demo/reports/rerun_demo_result.json`):

* Silver identisk (alla kolumner, inklusive tidsstämplar) efter steg 2, 3 och 4.
* Gold identisk (utom `built_by_run_id`) efter steg 2, 3 och 4.
* Bronze-antalet oförändrat i steg 4.
* Steg 3 och 4 rapporterar 0 nya, 0 ändrade, 0 raderade.
* Batch B ger exakt 1 ny, 1 ändrad, 1 oförändrad, 1 raderad.
* Silver har 5 rader varav 4 aktiva; inga dubletter av `record_id`.

## Köra i Fabric

I `00_Run_All`, parametercellen:

```python
source_mode = "demo"
run_mode = "rerun_demo"
```

Run all. Sista cellen visar Gold-tabellerna `diva_demo_gold_*`.

Enskild batch utan återkörningslogik: `source_mode = "demo"`, `run_mode = "pipeline"`,
`demo_batch = "A"` (eller `"B"`). Fungerar i både 00 och 01–04.

## Köra lokalt

```bash
pip install -e ".[dev,spark]"     # kräver Java 17+
python scripts/run_local_demo.py --workdir .local-demo
```

## Resultat från lokal körning (syntetiskt)

Lokal körning i byggmiljön (Spark 3.5.3, Delta 3.2.1, Python 3.11) gav:

| Steg | new | changed | unchanged | deleted | Bronze-rader totalt |
|---|---|---|---|---|---|
| 1 batch A | 4 | 0 | 0 | 0 | 5 |
| 2 batch B | 1 | 1 | 1 | 1 | 9 |
| 3 batch B igen (nytt run_id) | 0 | 0 | 4 | 0 | 13 |
| 4 samma run_id igen | 0 | 0 | 4 | 0 | 13 |

(`unchanged = 4` i steg 3–4 inkluderar den redan raderade demo-0004.)
Alla sju kontroller: PASS. Detta är syntetiska demovärden, inte mätningar av DiVA.
