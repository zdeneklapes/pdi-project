from dataclasses import dataclass, field
from pathlib import Path

from lib.mylogger import MyLogger


@dataclass()
class Program:
    args: dict = field(default_factory=dict)
    logger: MyLogger | None = field(default_factory=None)
    ROOT_DIR = Path(__file__).parent.parent
    SRC_DIR = ROOT_DIR / "src"
    DEBUG = True


