from pathlib import Path

import numpy as np
import xgboost as xgb

from wikiguard_ml import parity
from wikiguard_ml.versions import pinned


def read_matrix(path: Path) -> np.ndarray:
    return np.loadtxt(path, delimiter=",", dtype=np.float32, ndmin=2)


def test_installed_xgboost_is_the_pinned_version() -> None:
    assert xgb.__version__ == pinned("shared.xgboost")


def test_fixture_has_one_score_per_row(tmp_path: Path) -> None:
    parity.write_fixture(tmp_path)

    rows = read_matrix(tmp_path / parity.ROWS_FILE)
    scores = np.loadtxt(tmp_path / parity.SCORES_FILE, dtype=np.float32)

    assert rows.shape == (parity.ROW_COUNT, parity.FEATURE_COUNT)
    assert scores.shape == (parity.ROW_COUNT,)
    assert np.isnan(rows).any()
    assert ((scores > 0.0) & (scores < 1.0)).all()
    # A constant model would make the parity check meaningless.
    assert scores.max() - scores.min() > 0.5


def test_saved_files_reproduce_the_scores(tmp_path: Path) -> None:
    parity.write_fixture(tmp_path)

    booster = xgb.Booster(model_file=tmp_path / parity.MODEL_FILE)
    rows = read_matrix(tmp_path / parity.ROWS_FILE)
    expected = np.loadtxt(tmp_path / parity.SCORES_FILE, dtype=np.float32)

    assert np.array_equal(booster.predict(xgb.DMatrix(rows)), expected)


def test_fixture_is_deterministic(tmp_path: Path) -> None:
    parity.write_fixture(tmp_path / "a")
    parity.write_fixture(tmp_path / "b")

    first = (tmp_path / "a" / parity.SCORES_FILE).read_text()
    assert first == (tmp_path / "b" / parity.SCORES_FILE).read_text()
