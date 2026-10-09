"""Run the SYNTHETIC re-run demo locally with Spark + Delta (no Fabric needed).

Usage:  python scripts/run_local_demo.py [--workdir DIR]

A local directory plays the role of the Lakehouse (<workdir>/lakehouse/Files)
and a local Spark warehouse holds the Delta tables (<workdir>/warehouse).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from diva_lakehouse.config import PipelineConfig  # noqa: E402
from diva_lakehouse.pipeline import run_rerun_demo  # noqa: E402
from diva_lakehouse.spark_utils import get_spark  # noqa: E402
from diva_lakehouse.storage import Lakehouse  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default=None)
    args = ap.parse_args()
    workdir = os.path.abspath(args.workdir or tempfile.mkdtemp(prefix="diva-demo-"))
    root = os.path.join(workdir, "lakehouse")
    os.makedirs(os.path.join(root, "Files"), exist_ok=True)
    spark = get_spark(local_warehouse=os.path.join(workdir, "warehouse"))
    spark.sparkContext.setLogLevel("ERROR")
    cfg = PipelineConfig(source_mode="demo", run_mode="rerun_demo", lakehouse_root=root)
    result = run_rerun_demo(spark, cfg, Lakehouse(root))
    print(f"\nLocal demo workdir: {workdir}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
