"""01_Ingestion: harvest OAI-PMH pages, store them unchanged, write a manifest.

Raw pages are stored byte-for-byte under <files_base>/raw/<run_id>/ and the
manifest records SHA-256 per file, the request parameters and whether the
harvest is complete. A harvest stopped at max_records / max_pages is marked
complete=False and must never be treated as a finished interval.
"""

from __future__ import annotations

from collections import Counter

from .config import PipelineConfig, iso_utc, utc_now
from .oai_client import OAIClient, OAIProtocolError
from .selection import classify_record
from .storage import Lakehouse, sha256_bytes

MANIFEST_NAME = "manifest.json"


def raw_dir(cfg: PipelineConfig, run_id: str) -> str:
    return f"{cfg.files_base}/raw/{run_id}"


def manifest_path(cfg: PipelineConfig, run_id: str) -> str:
    return f"{raw_dir(cfg, run_id)}/{MANIFEST_NAME}"


def harvest(cfg: PipelineConfig, lakehouse: Lakehouse, client: OAIClient, run_id: str,
            from_datestamp: str | None, until_datestamp: str | None, log=print) -> dict:
    started = utc_now()
    log(f"Identify {client.endpoint} ...")
    identify = client.identify()
    log(f"  repository={identify.get('repositoryName')!r} granularity={identify.get('granularity')} "
        f"deletedRecord={identify.get('deletedRecord')} earliestDatestamp={identify.get('earliestDatestamp')}")
    if from_datestamp and "T" in from_datestamp and identify.get("granularity") == "YYYY-MM-DD":
        raise ValueError("The repository only supports day granularity; use YYYY-MM-DD datestamps.")

    requested = {
        "metadataPrefix": cfg.metadata_prefix,
        "from": from_datestamp,
        "until": until_datestamp,
        "set": cfg.set_spec or None,
        "max_records": cfg.max_records,
        "max_pages": cfg.max_pages,
        "thesis_filter": cfg.thesis_filter,
    }
    pages_meta = []
    used_total = 0
    in_raw_total = 0
    deleted = selected = not_selected = invalid = 0
    type_counter = Counter()
    reason_counter = Counter()
    complete = False
    stop_reason = None
    last_token = None
    oai_error = None

    for page in client.iter_list_records(cfg.metadata_prefix, from_datestamp, until_datestamp,
                                         cfg.set_spec or None, cfg.max_pages):
        fname = f"page_{page.page_number:05d}.xml"
        rel = f"{raw_dir(cfg, run_id)}/{fname}"
        lakehouse.write_bytes(rel, page.body)
        meta = {
            "file": fname,
            "path": lakehouse.display(rel),
            "sha256": sha256_bytes(page.body),
            "fetched_at_utc": iso_utc(utc_now()),
            "bytes": len(page.body),
            "url": page.url,
            "request_params": page.request_params,
            "http_status": page.http_status,
            "attempts": page.attempts,
            "parse_error": page.parse_error,
            "records_in_page": 0,
            "records_used": 0,
            "resumption_token": None,
            "complete_list_size": None,
            "oai_errors": [],
        }
        pages_meta.append(meta)
        if page.parsed is None:
            stop_reason = "page_parse_error"
            log(f"  page {page.page_number}: NOT parseable ({page.parse_error}); stopping (incomplete).")
            break
        parsed = page.parsed
        meta["oai_errors"] = [{"code": e.code, "message": e.message} for e in parsed.errors]
        meta["resumption_token"] = parsed.resumption_token
        meta["complete_list_size"] = parsed.complete_list_size
        if parsed.no_records_match:
            complete, stop_reason = True, "noRecordsMatch"
            log("  noRecordsMatch: the interval/set contains no records (complete, 0 records).")
            break
        if parsed.fatal_errors:
            stop_reason = "oai_error"
            oai_error = OAIProtocolError(parsed.fatal_errors, page.url)
            break

        n = len(parsed.records)
        remaining = cfg.max_records - used_total
        use = min(n, remaining)
        meta["records_in_page"] = n
        meta["records_used"] = use
        in_raw_total += n
        used_total += use
        for rec in parsed.records[:use]:
            if not rec.is_valid:
                invalid += 1
                continue
            if rec.deleted:
                deleted += 1
            for t in rec.dc.get("type", []):
                type_counter[" ".join(t.split())] += 1
            is_sel, reason = classify_record(rec.dc.get("type", []), rec.deleted, cfg.thesis_filter)
            reason_counter[reason] += 1
            if is_sel and not rec.deleted:
                selected += 1
            elif not is_sel:
                not_selected += 1
        log(f"  page {page.page_number}: {n} records (used {use}); total used {used_total}"
            + (f"; completeListSize={parsed.complete_list_size}" if parsed.complete_list_size else ""))
        last_token = parsed.resumption_token
        if used_total >= cfg.max_records and (use < n or last_token):
            stop_reason = "max_records"
            break
        if not last_token:
            complete, stop_reason = True, "list_exhausted"
            break
    else:
        # The page generator ended without a break: max_pages was reached
        # while a resumptionToken was still outstanding (or no page at all).
        stop_reason = "max_pages" if pages_meta else "no_pages"
        complete = False

    manifest = {
        "run_id": run_id,
        "source_mode": cfg.source_mode,
        "endpoint": client.endpoint,
        "identify": identify,
        "requested": requested,
        "fetch_started_at_utc": iso_utc(started),
        "fetch_finished_at_utc": iso_utc(utc_now()),
        "pages": pages_meta,
        "counts": {
            "pages": len(pages_meta),
            "records_in_raw_pages": in_raw_total,
            "records_fetched": used_total,  # records within max_records that are processed
            "records_beyond_max_records": in_raw_total - used_total,
            "selected_theses": selected,
            "not_selected": not_selected,
            "deleted_headers": deleted,
            "invalid_records": invalid,
        },
        "complete": complete,
        "stop_reason": stop_reason,
        "selection": {
            "rule": "dc:type pattern match (see selection.py / docs/data-dictionary.md)",
            "reasons": dict(reason_counter),
            "dc_type_values": dict(type_counter.most_common(50)),
        },
        "notes": [],
    }
    if cfg.source_mode == "demo":
        manifest["notes"].append("SYNTHETIC DEMO DATA - not real DiVA records.")
    if not complete:
        manifest["notes"].append(
            f"Harvest INCOMPLETE (stop_reason={stop_reason}). The interval is not registered as completed; "
            "narrow the datestamp interval or raise max_records and run again.")
    if cfg.source_mode == "live" and selected < cfg.low_selection_threshold and stop_reason != "oai_error":
        manifest["notes"].append(
            f"Few student theses selected ({selected}). Possible reasons: the datestamp interval is short; "
            "most changed records in the interval are not student theses; no set_spec restricts the harvest to "
            "theses; or the dc:type vocabulary differs from the documented patterns (see 'dc_type_values'). "
            "No data is fabricated to compensate.")
    lakehouse.write_json(manifest_path(cfg, run_id), manifest)
    for note in manifest["notes"]:
        log("  NOTE: " + note)
    if oai_error is not None:
        raise oai_error
    return manifest
