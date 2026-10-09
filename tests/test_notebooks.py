"""Generated notebooks are up to date, valid, and the embedded package imports."""

import importlib
import sys

import build_notebooks as B
import verify_notebooks as V


def test_generated_notebooks_are_valid_and_up_to_date():
    assert V.verify() == []


def test_logical_ids_are_stable_and_unique():
    ids = [B.logical_id(n) for n, _ in B.NOTEBOOKS]
    assert len(set(ids)) == len(ids)
    assert B.logical_id("00_Run_All") == "a84a0f7a-b0c3-53d2-9b63-e152e82d3950"


def test_embedded_package_works_without_src_on_path():
    """Execute the loader + module cells like Fabric would, then use the package."""
    cells = B.notebook_cells("00_Run_All")
    saved = {k: v for k, v in sys.modules.items() if k == "diva_lakehouse" or k.startswith("diva_lakehouse.")}
    saved_path = list(sys.path)
    saved_meta = list(sys.meta_path)
    try:
        for k in saved:
            del sys.modules[k]
        sys.path[:] = [p for p in sys.path if not p.rstrip("/").endswith("src")]
        ns = {}
        exec(cells[0], ns)  # parameters
        exec(cells[1], ns)  # loader
        for code in cells[2:2 + len(B.MODULES)]:
            exec(code, ns)
        cleaning = importlib.import_module("diva_lakehouse.cleaning")
        assert "<diva_lakehouse:" in cleaning.content_hash.__code__.co_filename
        assert cleaning.normalize_language(["swe"])[0] == "sv"
        cfg_mod = importlib.import_module("diva_lakehouse.config")
        assert cfg_mod.PipelineConfig(source_mode=ns["source_mode"]).max_records == 500
        importlib.import_module("diva_lakehouse.pipeline")  # imports every other module
    finally:
        for k in [m for m in sys.modules if m == "diva_lakehouse" or m.startswith("diva_lakehouse.")]:
            del sys.modules[k]
        sys.modules.update(saved)
        sys.path[:] = saved_path
        sys.meta_path[:] = saved_meta
