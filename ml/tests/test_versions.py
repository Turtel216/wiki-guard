from pathlib import Path

import pytest

from wikiguard_ml.versions import pinned


def test_reads_a_pinned_version(tmp_path: Path) -> None:
    versions_file = tmp_path / "versions.toml"
    versions_file.write_text('[shared]\nxgboost = "1.2.3"\n')

    assert pinned("shared.xgboost", versions_file) == "1.2.3"


def test_missing_key_is_an_error(tmp_path: Path) -> None:
    versions_file = tmp_path / "versions.toml"
    versions_file.write_text('[shared]\nxgboost = "1.2.3"\n')

    with pytest.raises(KeyError):
        pinned("shared.lightgbm", versions_file)


def test_repository_file_pins_the_shared_xgboost_version() -> None:
    assert pinned("shared.xgboost")
