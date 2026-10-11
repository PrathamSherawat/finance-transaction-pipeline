"""Settings: CLI flag > FIN_<NAME> environment variable > config file > default."""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path

DEFAULT_CONFIG_PATH = "config/pipeline.toml"
ENV_PREFIX = "FIN_"


@dataclass(frozen=True)
class Settings:
    input_csv: str = "data/sample/transactions.csv"
    incoming_dir: str = "data/incoming"
    state_dir: str = "data/state"
    output_dir: str = "data/output"
    quarantine_dir: str = "data/quarantine"
    sql_dir: str = "sql"
    log_level: str = "INFO"


def load_settings(path=DEFAULT_CONFIG_PATH, env=None) -> Settings:
    """Read the TOML file if present (missing file is fine), then apply env overrides.

    CLI flags win over all of this because they are argparse values whose defaults
    come from here.
    """
    env = os.environ if env is None else env
    names = {f.name for f in fields(Settings)}
    values: dict[str, str] = {}
    p = Path(path)
    if p.is_file():
        data = tomllib.loads(p.read_text(encoding="utf-8-sig"))
        unknown = sorted(set(data) - names)
        if unknown:
            raise ValueError(f"unknown setting(s) in {p}: {unknown}")
        for key, value in data.items():
            if not isinstance(value, str):
                raise ValueError(f"setting {key} in {p} must be a string, got {value!r}")
            values[key] = value
    for name in names:
        raw = env.get(ENV_PREFIX + name.upper())
        if raw:
            values[name] = raw
    return Settings(**values)
