# Uppsättning i Microsoft Fabric

Den här guiden beskriver de manuella stegen från GitHub-repo till en körd pipeline.
Ingenting här har körts i Fabric av författaren av koden: Fabric-körningen är
**inte verifierad**. Det som är verifierat lokalt står i [README](../README.md#verifiering).

## Vad Git-synken gör och inte gör

Fabric Git-integration synkar **item-definitioner** från en katalog i repot till
ett workspace. I det här projektet är det fem notebook-items under `fabric/`:

```
fabric/
├── 00_Run_All.Notebook/    (.platform + notebook-content.py)
├── 01_Ingestion.Notebook/
├── 02_Bronze.Notebook/
├── 03_Silver.Notebook/
└── 04_Gold.Notebook/
```

Git-synken:

* skapar/uppdaterar de fem notebookarna i workspacet,
* **installerar inga bibliotek** (det behövs inte: all kod är inbäddad i varje notebook
  och använder bara Fabrics inbyggda Spark/Delta-runtime och Pythons standardbibliotek),
* **skapar inget Lakehouse och kopplar inget Lakehouse** till notebookarna
  (notebook-metadata innehåller medvetet inga Lakehouse- eller workspace-ID),
* **kör ingenting**.

Tabeller, filer under `Files/diva/...`, exporter och körningsrapporter skapas först när
du kör en notebook.

Filformatet följer Microsofts dokumentation (lästa 2026-10-09):

* [Git integration source code format](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/source-code-format):
  katalognamn `{displayName}.{typ}`, systemfilen `.platform` (version 2.0) med `logicalId`.
* [Notebook source control](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-source-control-deployment):
  PySpark-notebooks lagras som `notebook-content.py`.
* [Notebook definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/notebook-definition):
  `# Fabric notebook source`, `# METADATA` / `# META`, `# CELL`, kernel `synapse_pyspark`,
  `.platform` med `$schema .../gitIntegration/platformProperties/2.0.0/schema.json`.

`logicalId` i varje `.platform` är ett stabilt UUID (uuid5 av notebooknamnet, se
`scripts/build_notebooks.py`). Ändra det inte efter första synken. Endast cell-markörer
som visas i dokumentationen används (`# CELL` och `# METADATA`); förklaringar ligger därför
som kommentarer i kodcellerna. Parametercellen är en vanlig kodcell och är inte markerad
som Fabric-"parameter cell" (det behövs bara för pipeline-parametrar).

## Förutsättningar

* Ett Fabric-workspace på en kapacitet (Fabric-, trial- eller Premium-kapacitet) där du
  får skapa Lakehouse och notebooks.
* Behörighet att ansluta workspacet till GitHub (GitHub stöds för Git-integration enligt
  [Introduction to Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration)),
  och ett GitHub-konto/token med åtkomst till repot. Exakt var anslutningen görs i UI:t kan
  ändras; följ Microsofts aktuella guide "Get started with Git integration".
* Att Fabric-kapaciteten tillåter utgående HTTPS till DiVA (`www.diva-portal.org`) för
  live-läget.

## Steg för steg

1. **Pusha repot till GitHub** (t.ex. branch `main`).
2. **Skapa ett workspace** och tilldela det en kapacitet (Workspace settings → License info).
3. **Anslut workspacet till GitHub**: Workspace settings → Git integration → GitHub.
   Välj konto/anslutning, repository, **branch** och **Git folder = `fabric`**
   (katalogen i repot som innehåller `*.Notebook`-mapparna, inte repo-roten).
4. **Synka**: när anslutningen sparas erbjuder Fabric att synka från Git till workspacet
   (eller använd Source control → Updates → Update all). De fem notebookarna ska dyka upp.
5. **Skapa eller välj ett Lakehouse och koppla det**:
   * Skapa ett Lakehouse i workspacet (New item → Lakehouse), t.ex. `diva_lakehouse`.
     Både Lakehouse med och utan schemas fungerar; tabellerna skapas i standardschemat.
   * Öppna `00_Run_All` → i Explorer-panelen: **Add data items / Lakehouses → Existing
     Lakehouse** → välj ditt Lakehouse. Det första Lakehouse du lägger till blir default
     (kontrollera att det är markerat/pinnat som default).
   * Om du vill köra 01–04 separat: koppla **samma** Lakehouse som default i var och en.
6. **Kör**: öppna `00_Run_All`, kontrollera parametrarna i första cellen och välj **Run all**.

Notebooken kontrollerar tidigt att `/lakehouse/default/Files` finns och är skrivbar.
Saknas default-Lakehouse stoppar den med ett felmeddelande som säger vad du ska göra.

### Första körning: rekommenderad ordning

1. **Demo först (valfritt men snabbt):** sätt `source_mode = "demo"` och
   `run_mode = "rerun_demo"` och kör `00_Run_All`. Kör den SYNTETISKA demon
   A → B → B igen → samma run_id igen och verifierar sig själv. Skriver bara till
   tabeller `diva_demo_*` och `Files/diva/demo/`.
2. **Live:** sätt tillbaka `source_mode = "live"`, `run_mode = "pipeline"`. Standard är
   datestamp-intervallet 2024-03-01 – 2024-03-02 och högst 500 poster.

## Endpoint och nätverk – ej verifierat

Standard-endpoint är `https://www.diva-portal.org/dice/oai` (kandidat från uppdraget).
Den **kunde inte nås från byggmiljön** (anslutningen återställdes av miljöns proxy, och ett
hämtningsverktyg fick HTTP 503), så varken `Identify`, `ListRecords`, granulariteten, set-namn
eller DiVA:s faktiska `dc:type`-värden är verifierade. Därför:

* `01_Ingestion`/`00_Run_All` anropar alltid `Identify` först och stoppar med ett tydligt
  fel om endpointen inte svarar som en OAI-PMH 2.0-tjänst.
* Endpointen är en parameter (`oai_endpoint`). Hittar du en annan officiell DiVA
  OAI-PMH-adress (t.ex. via DiVA:s dokumentation eller OpenDOAR), byt den där.
* Testa manuellt i en webbläsare:
  `<endpoint>?verb=Identify`, `<endpoint>?verb=ListMetadataFormats`, `<endpoint>?verb=ListSets`.
* Om Identify visar `granularity = YYYY-MM-DD` måste datum anges som `YYYY-MM-DD`
  (standard). Sekundformat avvisas då med ett felmeddelande.
* Om manifestet visar få valda uppsatser: titta på `selection.dc_type_values` i
  `Files/diva/raw/<run_id>/manifest.json` och justera vid behov mönstren i
  `src/diva_lakehouse/selection.py` (bygg sedan om notebookarna, se nedan), eller ange
  ett `set_spec` om DiVA har ett set för studentuppsatser.

## Alternativ: import av .ipynb utan Git

`ipynb/` innehåller samma notebooks som vanliga Jupyter-filer (samma kodceller).

1. I workspacet: **Import → Notebook → From this computer** och välj de fem
   `.ipynb`-filerna.
2. Koppla default-Lakehouse (steg 5 ovan) och kör `00_Run_All`.

## Ändra koden

Redigera aldrig `fabric/` eller `ipynb/` för hand. Ändra i `src/diva_lakehouse/` och kör:

```bash
python scripts/build_notebooks.py      # genererar fabric/ och ipynb/
python scripts/verify_notebooks.py     # validerar struktur, logicalId och att allt är aktuellt
```

Committa, pusha och välj **Update** i Fabric (Source control). Om du har ändrat en notebook
i Fabric och committat därifrån kan Fabric skriva om metadata i `notebook-content.py`;
`verify_notebooks.py` flaggar då att filerna avviker från generatorn.

## Felsökning

| Symptom | Orsak / åtgärd |
|---|---|
| `No default Lakehouse is available` | Koppla ett Lakehouse som default (steg 5) och starta om sessionen. |
| `HTTP 0 ... network error` / timeout vid Identify | Endpoint nås inte från Fabric. Kontrollera URL och nätverksregler. |
| `OAI-PMH error ... badArgument` | Datumformat/granularitet eller `set_spec` stämmer inte med Identify/ListSets. |
| `Harvest INCOMPLETE (stop_reason=max_records)` | Intervallet hade fler poster än `max_records`. Korta intervallet eller höj gränsen (max 1000). Intervallet registreras inte som klart. |
| `Blocking data quality checks failed` | Gold och exporter har inte ersatts. Läs `Files/diva/reports/<run_id>.json`. |
| 02–04 hittar ingen körning | Kör 01 först, eller ange `run_id`. |
