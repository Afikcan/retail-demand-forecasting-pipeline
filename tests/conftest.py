import importlib.util
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("MPLBACKEND", "Agg")  # headless for matplotlib in 06_evaluate.py

ROOT = Path(__file__).resolve().parents[1]


def load_module(rel_path, name):
    """Loads a pipeline .py file as an importable module.

    The pipeline scripts are numbered (01_ingest.py, etc.) so they aren't
    valid package names — load them directly from file path instead.
    """
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("tests").getOrCreate()
