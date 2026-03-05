"""ADO connector configuration."""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AdoConfig:
    dumps_dir: Path
