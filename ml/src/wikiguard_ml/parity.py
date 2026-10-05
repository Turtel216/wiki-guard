"""Writes the fixture for the Python to JVM scoring parity test.

A tiny model is trained on synthetic data and saved in the native XGBoost
format, together with the rows it was scored on and the scores Python got.
The JVM test loads the same model, scores the same rows and must agree.
"""

import sys
from pathlib import Path

import numpy as np
import numpy.typing as npt
import xgboost as xgb

MODEL_FILE = "model.ubj"
ROWS_FILE = "rows.csv"
SCORES_FILE = "scores.csv"

ROW_COUNT = 500
FEATURE_COUNT = 6
MISSING_SHARE = 0.1
SEED = 7

FloatMatrix = npt.NDArray[np.float32]
FloatVector = npt.NDArray[np.float32]


def synthetic_rows() -> tuple[FloatMatrix, FloatVector]:
    """Build a small imbalanced dataset with some missing values."""
    rng = np.random.default_rng(SEED)
    features = rng.normal(size=(ROW_COUNT, FEATURE_COUNT)).astype(np.float32)
    signal = features[:, 0] * features[:, 1] + features[:, 2]
    labels = (signal + rng.normal(scale=0.5, size=ROW_COUNT) > 1.0).astype(np.float32)
    # Missing values go in after the labels, so the model has to route them.
    features[rng.random(features.shape) < MISSING_SHARE] = np.nan
    return features, labels


def train(features: FloatMatrix, labels: FloatVector) -> xgb.Booster:
    params = {
        "objective": "binary:logistic",
        "tree_method": "hist",
        "max_depth": 3,
        "eta": 0.3,
        "seed": SEED,
        "nthread": 1,
    }
    return xgb.train(params, xgb.DMatrix(features, label=labels), num_boost_round=20)


def format_float(value: np.float32) -> str:
    """Render a float32 so that parsing it back gives the same float32."""
    return "NaN" if np.isnan(value) else repr(float(value))


def write_fixture(out_dir: Path) -> None:
    """Train the model and write model, rows and expected scores to out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    features, labels = synthetic_rows()
    booster = train(features, labels)
    scores: FloatVector = booster.predict(xgb.DMatrix(features))

    booster.save_model(out_dir / MODEL_FILE)
    row_lines = [",".join(format_float(value) for value in row) for row in features]
    (out_dir / ROWS_FILE).write_text("\n".join(row_lines) + "\n")
    (out_dir / SCORES_FILE).write_text("\n".join(format_float(s) for s in scores) + "\n")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m wikiguard_ml.parity <output directory>")
    write_fixture(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
