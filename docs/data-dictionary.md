# Datakatalog

Tabellnamn har prefix `diva_` (live) eller `diva_demo_` (demo). Alla tidpunkter är UTC.

## Källfält (OAI-PMH + Dublin Core, `oai_dc`)

| Källa | Används till |
|---|---|
| `header/identifier` | `record_id` (OAI-identifierare, trimmad) |
| `header/datestamp` | `source_datestamp` – när posten skapades/ändrades i källan. **Inte** publiceringsår. |
| `header/@status="deleted"` | `deleted` / `is_deleted` |
| `header/setSpec` | `set_specs` |
| `dc:title` | `title` (första), `titles` (alla) |
| `dc:description` | `abstract` (första), `abstracts` (alla) |
| `dc:subject` | `subjects` |
| `dc:date` | `publication_year` (tolkat), `date_raw` (original) |
| `dc:language` | `language` (normaliserad), `language_raw` |
| `dc:publisher` | `publishers`, härledning av lärosäte |
| `dc:type` | `document_types`, urval av studentuppsatser |
| `dc:identifier` | `dc_identifiers`, `source_url` |

`dc:creator` och `dc:contributor` (personnamn) hämtas inte vidare till tabellerna. De finns
kvar i de oförändrade rå-XML-filerna.

Fältnamnen ovan är OAI-PMH/Dublin Core-standard. Hur DiVA fyller dem (t.ex. vilka `dc:type`-
värden som används och om `dc:publisher` är lärosäte eller institution) har **inte**
kunnat verifieras mot den levande endpointen från byggmiljön.

## Urval av studentuppsatser (`selection.py`)

En post väljs om något `dc:type`-värde (gemener, normaliserat whitespace) innehåller något av:
`student thesis`, `studentthesis`, `studentuppsats`, `student paper`, `examensarbete`,
`independent thesis`, `bachelorthesis`, `bachelor thesis`, `masterthesis`, `master thesis`,
`kandidatuppsats`, `magisteruppsats`, `masteruppsats`.

Uteslutna även om ovanstående matchar: `doctoral thesis`, `doctoralthesis`,
`doktorsavhandling`, `licentiate thesis`, `licentiatethesis`, `licentiatavhandling`.

Alla poster (även ej valda) sparas i Bronze med `is_selected` och `selection_reason`.
Manifestet redovisar antal per orsak och de 50 vanligaste `dc:type`-värdena. Deleted headers
saknar metadata och behålls alltid så att en tidigare vald post kan markeras som raderad.
`thesis_filter = "none"` (endast i `PipelineConfig`) stänger av filtret.

## Bronze: `<prefix>_bronze_records`

Korn: en observation av en post i en körning (`observation_id`).

| Kolumn | Typ | Beskrivning |
|---|---|---|
| observation_id | string | sha256(run_id \| raw_file_sha256 \| record_index) |
| record_id | string | OAI-identifierare |
| source_datestamp / source_datestamp_raw | timestamp / string | OAI datestamp, tolkad och original |
| set_specs | array<string> | setSpec |
| deleted | boolean | header status="deleted" |
| is_selected / selection_reason | boolean / string | urvalsregel och orsak |
| dc_title, dc_subject, dc_description, dc_publisher, dc_date, dc_type, dc_identifier, dc_language | array<string> | källvärden oförändrade (inklusive whitespace) |
| raw_file_path / raw_file_sha256 / record_index | string / string / int | spårbarhet till råfilen |
| fetched_at | timestamp | när sidan hämtades |
| run_id, source_mode, endpoint | string | körning |
| harvest_complete | boolean | om körningens hämtning var komplett |
| ingested_at | timestamp | när Bronze-raden byggdes |

## Karantän: `<prefix>_bronze_quarantine`

`quarantine_id`, `run_id`, `raw_file_path`, `raw_file_sha256`, `record_index` (−1 = hela sidan),
`oai_identifier`, `source_datestamp_raw`, `error_reason`, `raw_snippet` (max 4000 tecken),
`fetched_at`, `ingested_at`.

Orsaker: `missing header identifier`, `missing header datestamp`, `invalid header datestamp`,
`missing oai_dc metadata on non-deleted record`, `page not parseable`, `OAI error page`.

## Silver: `<prefix>_silver_theses`

Korn: en rad per `record_id` (aktuell version).

| Kolumn | Regel |
|---|---|
| title, titles | NFC, allt whitespace → ett mellanslag, trim; tomma tas bort, dubletter tas bort (ordning behålls) |
| abstract, abstracts | Endast whitespace normaliseras: radbrytningar → `\n`, mellanslag/tab i rad → ett, rader trimmas, 3+ radbrytningar → en tomrad. Ord och ordning ändras aldrig. |
| subjects | lista, normaliserad text, unika |
| publication_year (int), publication_year_status | Se nedan |
| date_raw | originalvärden från `dc:date` |
| language, language_codes, language_raw | Se nedan |
| publishers | lista, normaliserad text |
| university, university_group, university_method, university_candidates | Se nedan |
| document_types | `dc:type`-värden |
| dc_identifiers, source_url | `source_url` = första http(s)-identifier, helst på diva-portal.org; annars null |
| set_specs | setSpec |
| content_hash | Se nedan |
| is_deleted | true efter deleted header |
| source_datestamp, source_observation_id, source_raw_file_path, source_raw_file_sha256, source_run_id | observationen som senast ändrade raden |
| first_seen_at, first_seen_run_id | första insättningen (behålls alltid) |
| last_changed_at, last_changed_run_id | ändras bara när innehåll eller raderingsstatus ändras |

### Publiceringsår (försiktig tolkning)

Endast värden av formen `YYYY`, `YYYY-MM`, `YYYY-MM-DD` (ev. med tid) accepteras, året måste ligga
i [1900, innevarande år + 1]. Status: `parsed`; `missing` (inget `dc:date`); `unparseable`
(t.ex. "ca 1990"); `ambiguous` (flera olika år → `publication_year` = null).
Originalet finns alltid i `date_raw`.

### Språk

| Original (skiftlägesokänsligt) | Kod |
|---|---|
| sv, swe, sve, sv-se, svenska, swedish | sv |
| en, eng, en-gb, en-us, engelska, english | en |
| no, nor, nb, nob, nn, nno, norska, norwegian | no |
| da, dan, danska, danish | da |
| fi, fin, finska, finnish | fi |
| de, ger, deu, tyska, german | de |
| fr, fre, fra, franska, french | fr |
| es, spa, spanska, spanish | es |
| it, ita, italienska, italian | it |
| övrigt | und |

`language` = första mappade koden, annars `und`. `language_raw` behåller originalen.

### Lärosäte (heuristik)

`dc:publisher` behandlas **inte** automatiskt som lärosäte. Härledning:

1. `publisher_match`: publisher-texten innehåller (ordgräns, skiftlägesokänsligt) ett av de
   dokumenterade namnmönstren i `cleaning.UNIVERSITY_PATTERNS` (svenska och engelska namn för
   24 lärosäten). Exakt ett lärosäte → `university`.
2. `multiple_publisher_matches`: flera olika lärosäten → `university = null`,
   `university_group = "Flera lärosäten (ej entydigt)"`, alla i `university_candidates`.
3. `identifier_prefix`: om ingen publisher matchar och identifieraren har formen
   `oai:DiVA.org:<kod>-<nummer>` med känd kod (t.ex. `uu`, `kth`, `liu`). Konventionen är inte
   verifierad mot levande data.
4. `unknown`: `university = null`, `university_group = "Okänt lärosäte"`.

### content_hash

SHA-256 av kanonisk JSON (sorterade nycklar, UTF-8) över de **rensade** affärsfälten
`record_id, title, titles, abstract, abstracts, subjects, date_raw, language_raw, publishers,
document_types, dc_identifiers`. `subjects`, `publishers`, `document_types`, `dc_identifiers`
sorteras först (ordning saknar betydelse). Ingår inte: `run_id`, tidpunkter, datestamp,
`set_specs`, härledda fält, raderingsstatus (spåras separat via `is_deleted`).
Ändringar som bara är whitespace räknas alltså inte som ändring.

## Gold

Alla Gold-tabeller byggs om helt från aktiva Silver-rader och har kolumnerna `data_scope`
(text som märker resultatet som begränsat urval eller syntetisk demo), `source_mode` och
`built_by_run_id`. Resultaten beskriver **endast** de poster detta projekt har hämtat
(datestamp-intervall × `max_records` × urvalsregel) och är inte statistik över hela DiVA eller
Sveriges uppsatser.

| Tabell | Korn | Kolumner |
|---|---|---|
| `<prefix>_gold_theses_by_year` | en rad per `publication_year` (null = okänt/tvetydigt år) | publication_year, thesis_count |
| `<prefix>_gold_theses_by_university_year` | en rad per (`university_group`, `publication_year`) | university_group, publication_year, thesis_count |
| `<prefix>_gold_metadata_completeness` | en rad per mått | metric, records_total, records_with_value, share |

**Flera lärosäten:** varje post räknas exakt en gång per tabell. Poster med flera matchade
lärosäten hamnar i gruppen "Flera lärosäten (ej entydigt)" och fördelas inte på varje lärosäte;
poster utan identifierat lärosäte hamnar i "Okänt lärosäte". Därför är summan av
`thesis_count` i `by_university_year` = summan i `by_year` = antal aktiva Silver-poster, vilket
kontrolleras blockerande innan Gold skrivs.

Kompletthetsmått: `title`, `abstract`, `subjects` (minst ett), `publication_year`,
`identified_university` (`university` ej null, dvs. exakt ett lärosäte).

## Exporter

`Files/diva/exports/` (live) och `Files/diva/demo/exports/` (demo):
`gold_theses_by_year.{json,csv}`, `gold_theses_by_university_year.{json,csv}`,
`gold_metadata_completeness.{json,csv}`, `export_metadata.json` (run_id, data_scope,
alla hämtade intervall med `complete`/`processed`).

## Hjälptabell: `<prefix>_harvest_log`

En rad per run_id: endpoint, set_spec, from/until, complete, stop_reason, records_fetched,
selected_theses, fetched_at, processed. Används för `from_datestamp = "auto"`.
