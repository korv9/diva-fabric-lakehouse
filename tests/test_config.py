import pytest

from diva_lakehouse.config import ConfigError, PipelineConfig


def test_defaults_are_live_historical_and_capped():
    cfg = PipelineConfig()
    assert cfg.source_mode == "live" and cfg.max_records == 500
    assert cfg.from_datestamp == "2024-03-01" and cfg.until_datestamp == "2024-03-02"
    assert cfg.table_prefix == "diva" and cfg.files_base == "diva"
    assert cfg.tables["silver"] == "diva_silver_theses"


def test_demo_uses_separate_tables_and_paths():
    cfg = PipelineConfig(source_mode="demo")
    assert cfg.table_prefix == "diva_demo" and cfg.files_base == "diva/demo"
    assert set(cfg.tables.values()).isdisjoint(PipelineConfig().tables.values())


@pytest.mark.parametrize("kw", [
    {"source_mode": "test"},
    {"source_mode": "demo", "table_prefix": "diva"},
    {"source_mode": "live", "table_prefix": "diva_demo"},
    {"run_mode": "rerun_demo"},  # needs demo
    {"max_records": 0},
    {"max_records": 5000},
    {"from_datestamp": "2024-13"},
    {"from_datestamp": "2024-03-05", "until_datestamp": "2024-03-01"},
    {"from_datestamp": "2024-03-01T00:00:00Z", "until_datestamp": "2024-03-02"},
    {"run_id": "bad run id"},
    {"oai_endpoint": "ftp://x"},
])
def test_invalid_parameters(kw):
    with pytest.raises(ConfigError):
        PipelineConfig(**kw)


def test_auto_from_is_allowed():
    assert PipelineConfig(from_datestamp="auto").from_datestamp == "auto"
