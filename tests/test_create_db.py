"""
test_create_db.py
~~~~~~~~~~~~~~~~~
"""
import sys
from pathlib import Path
import importlib.util

# some magic to import the script as a module
# see https://docs.python.org/3/library/importlib.html?highlight=import_module#importing-a-source-file-directly  # noqa

create_db_path = (Path(__file__).parent.parent / Path("src")
                  / Path("create_db.py"))
spec = importlib.util.spec_from_file_location(
    "create_db", str(create_db_path.absolute())
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
sys.modules["create_db"] = module
import create_db

import pytest


def test_chunks_with_equal_sizes():
    chunker = create_db.generate_chunks(list(range(10000)), 100)
    chunks = list(chunker)

    assert len(chunks) == 100
    for chunk in chunks:
        assert len(chunk) == 100


def test_chunks_with_unequal_sizes():
    chunker = create_db.generate_chunks(list(range(10000)), 101)
    chunks = list(chunker)
    assert len(chunks[-1]) == 1
    for chunk in chunks[:-1]:
        assert len(chunk) == 101


def test_invalid_chunksize():
    chunker = create_db.generate_chunks(list(range(1000)), -1)
    with pytest.raises(ValueError):
        next(chunker)

