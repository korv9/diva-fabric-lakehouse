"""Validate the generated Fabric notebook items and .ipynb files (no Fabric needed).

Checks:
  * generated files are up to date with src/ (same output as build_notebooks.py)
  * every fabric/<Name>.Notebook has exactly .platform + notebook-content.py
  * .platform: v2 schema URL, type Notebook, displayName == folder name,
    logicalId a valid, non-zero, unique UUID equal to the stable generated ID
  * notebook-content.py: Fabric source header, CELL/METADATA structure,
    synapse_pyspark kernel, every cell compiles, embedded modules == src/
  * ipynb: valid JSON, nbformat 4, same code cells as the Fabric notebook

Usage: python scripts/verify_notebooks.py
"""

from __future__ import annotations

import ast
import json
import os
import re
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_notebooks as B  # noqa: E402

CELL_MARK = "# CELL ********************"
META_MARK = "# METADATA ********************"


def parse_fabric_source(text: str) -> tuple:
    """Return (notebook_metadata, [(code, cell_metadata), ...])."""
    if not text.startswith("# Fabric notebook source\n"):
        raise AssertionError("missing '# Fabric notebook source' header")
    blocks = re.split(r"^(# CELL \*{20}|# METADATA \*{20})$", text, flags=re.M)
    # blocks: [header, MARK, body, MARK, body, ...]
    tokens = list(zip(blocks[1::2], blocks[2::2]))
    if not tokens or tokens[0][0] != META_MARK:
        raise AssertionError("notebook must start with a METADATA block")

    def meta(body):
        lines = [ln for ln in body.strip("\n").split("\n") if ln.strip()]
        if not all(ln.startswith("# META") for ln in lines):
            raise AssertionError("METADATA block contains non-META lines")
        return json.loads("\n".join(ln[len("# META"):].lstrip(" ") if ln != "# META" else "" for ln in lines))

    nb_meta = meta(tokens[0][1])
    cells = []
    rest = tokens[1:]
    if len(rest) % 2:
        raise AssertionError("every CELL must be followed by a METADATA block")
    for (m1, code), (m2, mbody) in zip(rest[0::2], rest[1::2]):
        if m1 != CELL_MARK or m2 != META_MARK:
            raise AssertionError("expected CELL followed by METADATA")
        cells.append((code.strip("\n"), meta(mbody)))
    return nb_meta, cells


def verify() -> list:
    errors = []
    expected = B.render_all()
    for rel, content in expected.items():
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            errors.append(f"missing generated file {rel}")
        elif open(path, encoding="utf-8").read() != content:
            errors.append(f"{rel} is out of date - run python scripts/build_notebooks.py")

    fabric_dir = os.path.join(ROOT, "fabric")
    item_dirs = sorted(d for d in os.listdir(fabric_dir) if d.endswith(".Notebook"))
    names = [n for n, _ in B.NOTEBOOKS]
    if item_dirs != [f"{n}.Notebook" for n in names]:
        errors.append(f"unexpected notebook folders: {item_dirs}")
    ids = []
    for name in names:
        d = os.path.join(fabric_dir, f"{name}.Notebook")
        if not os.path.isdir(d):
            continue
        files = sorted(os.listdir(d))
        if files != [".platform", "notebook-content.py"]:
            errors.append(f"{name}: unexpected files {files}")
        plat = json.load(open(os.path.join(d, ".platform"), encoding="utf-8"))
        if plat.get("$schema") != ("https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
                                   "platformProperties/2.0.0/schema.json"):
            errors.append(f"{name}: unexpected $schema")
        md, cfg = plat.get("metadata", {}), plat.get("config", {})
        if md.get("type") != "Notebook" or md.get("displayName") != name:
            errors.append(f"{name}: metadata.type/displayName wrong: {md}")
        if cfg.get("version") != "2.0":
            errors.append(f"{name}: config.version must be '2.0'")
        lid = cfg.get("logicalId", "")
        try:
            if uuid.UUID(lid) == uuid.UUID(int=0):
                errors.append(f"{name}: logicalId must not be the zero GUID")
        except ValueError:
            errors.append(f"{name}: logicalId is not a GUID: {lid!r}")
        if lid != B.logical_id(name):
            errors.append(f"{name}: logicalId changed (must stay stable)")
        ids.append(lid)

        text = open(os.path.join(d, "notebook-content.py"), encoding="utf-8").read()
        try:
            nb_meta, cells = parse_fabric_source(text)
        except (AssertionError, json.JSONDecodeError) as exc:
            errors.append(f"{name}: notebook-content.py structure: {exc}")
            continue
        if nb_meta.get("kernel_info", {}).get("name") != "synapse_pyspark":
            errors.append(f"{name}: kernel must be synapse_pyspark")
        if "lakehouse" in json.dumps(nb_meta.get("dependencies", {})).lower():
            errors.append(f"{name}: no Lakehouse IDs may be committed in notebook metadata")
        for i, (code, cmeta) in enumerate(cells):
            if cmeta != {"language": "python", "language_group": "synapse_pyspark"}:
                errors.append(f"{name}: cell {i} metadata {cmeta}")
            try:
                ast.parse(code)
            except SyntaxError as exc:
                errors.append(f"{name}: cell {i} does not compile: {exc}")
        # embedded module sources must equal src/ exactly
        for mod in B.MODULES:
            if B.module_cell(mod).rstrip("\n") not in [c for c, _ in cells]:
                errors.append(f"{name}: embedded module {mod} differs from src/")
        for c, _ in cells:
            if c.startswith("# Embedded module"):
                tree = ast.parse(c)
                src = tree.body[0].value.args[1].value
                ast.parse(src)
        # ipynb consistency
        nb = json.load(open(os.path.join(ROOT, "ipynb", f"{name}.ipynb"), encoding="utf-8"))
        if nb.get("nbformat") != 4:
            errors.append(f"{name}.ipynb: nbformat must be 4")
        code_cells = ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]
        if code_cells != [c for c, _ in cells]:
            errors.append(f"{name}.ipynb: code cells differ from the Fabric notebook")
        try:
            import nbformat

            nbformat.validate(nbformat.reads(json.dumps(nb), as_version=4))
        except ImportError:
            pass
        except Exception as exc:  # nbformat.ValidationError
            errors.append(f"{name}.ipynb: nbformat validation: {exc}")
    if len(set(ids)) != len(ids):
        errors.append("logicalIds are not unique")
    return errors


def main() -> int:
    errors = verify()
    if errors:
        print("Notebook verification FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print("Notebook verification passed: fabric/*.Notebook and ipynb/*.ipynb are valid and up to date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
