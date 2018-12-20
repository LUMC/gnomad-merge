"""
test_db_to_vcf.py
~~~~~~~~~~~~~~~~~
"""
import sys
from pathlib import Path
import importlib.util

import pytest


# some magic to import the script as a module
# see https://docs.python.org/3/library/importlib.html?highlight=import_module#importing-a-source-file-directly  # noqa

create_db_path = (Path(__file__).parent.parent / Path("src")
                  / Path("db_to_vcf.py"))
spec = importlib.util.spec_from_file_location(
    "db_to_vcf", str(create_db_path.absolute())
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
sys.modules["db_to_vcf"] = module
import db_to_vcf  # noqa


@pytest.fixture
def test_db_path():
    return Path(__file__).parent / Path("data") / Path("test.db")


test_af_data = [
    (0, 1, 0),
    (0, 0, None),
    (1, 10, 0.1),
    (15, 0, None)
]


test_tuple_vcf_data = [
    (
        ("1", 100, None, "A", "T", 100, 1, 10, None),
        "1\t100\t.\tA\tT\t100\tPASS\tAC=1;AF=0.1;AN=10"
    ),
    (
        ("1", 100, "rs_1", "A", "T", 100, 1, 10, "NOT_OK"),
        "1\t100\trs_1\tA\tT\t100\tNOT_OK\tAC=1;AF=0.1;AN=10"
    ),
    (
        ("1", 100, None, "A", "T", 100, 0, 0, None),
        "1\t100\t.\tA\tT\t100\tPASS\tAC=0;AF=.;AN=0"
    )
]


@pytest.mark.parametrize("ac, an, expected", test_af_data)
def test_af(ac, an, expected):
    assert db_to_vcf.calc_af(ac, an) == expected


@pytest.mark.parametrize("tupl, expected", test_tuple_vcf_data)
def test_tuple_vcf(tupl, expected):
    assert db_to_vcf.db_tuple_to_record(tupl) == expected


def test_generate_vcf(test_db_path):
    for x in db_to_vcf.transform_db_to_vcf(test_db_path):
        pass


def test_main(test_db_path):
    db_to_vcf.main(test_db_path)
