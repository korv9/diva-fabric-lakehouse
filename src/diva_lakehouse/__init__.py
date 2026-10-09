"""DiVA Research Lakehouse: OAI-PMH metadata -> Bronze/Silver/Gold Delta tables.

The package is pure Python apart from the Spark/Delta stages (bronze, silver,
gold, quality, pipeline). The Fabric notebooks embed this package verbatim; see
scripts/build_notebooks.py.
"""

__version__ = "0.1.0"
