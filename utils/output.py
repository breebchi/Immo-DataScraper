from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd


def write_json(data: Dict[str, Dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2)


def write_csv(data: Dict[str, Dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame.from_dict(data, orient="index")
    df.to_csv(path, index_label="id", encoding="utf-8")
