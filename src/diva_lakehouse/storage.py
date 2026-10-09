"""Lakehouse Files access through the default-Lakehouse file mount.

In a Fabric notebook with a default Lakehouse attached, the Lakehouse is
mounted at /lakehouse/default/ (documented in "Load data into your lakehouse
with a notebook"). Files are written with plain Python I/O under
/lakehouse/default/Files/...; Delta tables are created with saveAsTable in the
default Lakehouse. No workspace ID or ABFS path is needed.

Locally (tests, scripts/run_local_demo.py) any directory can act as the
Lakehouse root.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil

FABRIC_DEFAULT_LAKEHOUSE = "/lakehouse/default"

MISSING_LAKEHOUSE_HELP = (
    "No default Lakehouse is available to this notebook ({root} does not exist or has no Files folder).\n"
    "Fix: in the notebook, open the 'Explorer' / 'Lakehouses' pane, choose 'Add' (or 'Add data items'), "
    "select an existing Lakehouse or create a new one, and make sure it is pinned as the default Lakehouse. "
    "Then restart the session (Stop session) and run the notebook again. "
    "See docs/setup-fabric.md, step 5."
)


class LakehouseNotAvailable(RuntimeError):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Lakehouse:
    """Access to the Files section of a Lakehouse via a local (mounted) path."""

    def __init__(self, root: str = "auto"):
        self.root = FABRIC_DEFAULT_LAKEHOUSE if root in ("", "auto", None) else root

    @property
    def files_dir(self) -> str:
        return os.path.join(self.root, "Files")

    def check_available(self) -> None:
        """Fail early with an actionable message if no Lakehouse is attached."""
        if not os.path.isdir(self.files_dir):
            raise LakehouseNotAvailable(MISSING_LAKEHOUSE_HELP.format(root=self.root))
        probe = os.path.join(self.files_dir, ".diva_write_probe")
        try:
            with open(probe, "w", encoding="utf-8") as fh:
                fh.write("ok")
            os.remove(probe)
        except OSError as exc:
            raise LakehouseNotAvailable(
                f"The default Lakehouse Files folder is not writable: {exc}\n" + MISSING_LAKEHOUSE_HELP.format(root=self.root)
            ) from exc

    # ---------------------------------------------------------------- paths
    def path(self, rel: str) -> str:
        """Local path for a path relative to the Files section ('diva/raw/...')."""
        rel = rel.lstrip("/")
        if rel.startswith("Files/"):
            rel = rel[len("Files/"):]
        return os.path.join(self.files_dir, rel)

    @staticmethod
    def display(rel: str) -> str:
        """Lakehouse-relative path as shown in Fabric ('Files/diva/...')."""
        rel = rel.lstrip("/")
        return rel if rel.startswith("Files/") else "Files/" + rel

    def exists(self, rel: str) -> bool:
        return os.path.exists(self.path(rel))

    # ------------------------------------------------------------------- io
    def write_bytes(self, rel: str, data: bytes) -> str:
        p = self.path(rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, p)
        return p

    def read_bytes(self, rel: str) -> bytes:
        with open(self.path(rel), "rb") as fh:
            return fh.read()

    def write_json(self, rel: str, obj) -> str:
        data = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False, default=str).encode("utf-8")
        return self.write_bytes(rel, data)

    def read_json(self, rel: str):
        return json.loads(self.read_bytes(rel).decode("utf-8"))

    def write_text(self, rel: str, text: str) -> str:
        return self.write_bytes(rel, text.encode("utf-8"))

    def read_text(self, rel: str) -> str:
        return self.read_bytes(rel).decode("utf-8")

    def listdir(self, rel: str) -> list:
        p = self.path(rel)
        return sorted(os.listdir(p)) if os.path.isdir(p) else []

    def remove_tree(self, rel: str) -> None:
        p = self.path(rel)
        if os.path.isdir(p):
            shutil.rmtree(p)
