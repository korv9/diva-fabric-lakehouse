"""Run the GENERATED 00_Run_All notebook (Fabric source format) locally in demo mode.

This executes exactly the cells that Fabric will run, with two local
substitutions: the parameter values and the Lakehouse root (Fabric uses the
/lakehouse/default mount instead).
"""

import os
import re
import sys

import verify_notebooks as V

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_generated_run_all_notebook_rerun_demo(spark, lakehouse):
    text = open(os.path.join(ROOT, "fabric", "00_Run_All.Notebook", "notebook-content.py"), encoding="utf-8").read()
    _, cells = V.parse_fabric_source(text)
    params = cells[0][0]
    params = re.sub(r'^source_mode = "live"', 'source_mode = "demo"', params, flags=re.M)
    params = re.sub(r'^run_mode = "pipeline"', 'run_mode = "rerun_demo"', params, flags=re.M)
    params = re.sub(r'^table_prefix = ""', 'table_prefix = "nbtest_demo"', params, flags=re.M)
    saved_mods = {k: v for k, v in sys.modules.items() if k.startswith("diva_lakehouse")}
    saved_meta = list(sys.meta_path)
    ns = {"spark": spark}
    try:
        exec(params, ns)
        for code, _ in cells[1:]:
            if code.startswith("from diva_lakehouse import pipeline as P"):
                code += f"\ncfg.lakehouse_root = {lakehouse.root!r}\n"
            exec(code, ns)
        assert ns["demo_result"]["passed"]
        assert spark.table("nbtest_demo_silver_theses").count() == 5
    finally:
        for k in [m for m in sys.modules if m.startswith("diva_lakehouse")]:
            del sys.modules[k]
        sys.modules.update(saved_mods)
        sys.meta_path[:] = saved_meta
