"""
test_create_db.py
~~~~~~~~~~~~~~~~~
"""
import sys
from pathlib import Path
import importlib.util

import pytest
import cyvcf2
import tempfile


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
import create_db  # noqa


def mini_vcf_path():
    return Path(__file__).parent / Path("data") / Path("mini.vcf")


def mini_vcf(mini_vcf_path):
    return cyvcf2.VCF(str(mini_vcf_path))


@pytest.fixture
def missing_vcf():
    path = Path(__file__).parent / Path("data") / Path("missing.vcf")
    return cyvcf2.VCF(str(path))


@pytest.fixture()
def temp_db_conn():
    temp = tempfile.NamedTemporaryFile(delete=False)
    yield create_db.create_connection(Path(temp.name))
    temp.close()


@pytest.fixture()
def inited_conn(temp_db_conn):
    create_db.create_table(temp_db_conn)
    return temp_db_conn


__vcf_records = [x for x in mini_vcf(mini_vcf_path())]  # for parametrization

record_dicts_data = [
    (__vcf_records[0], {"chrom": "1", "pos": 17412, "id": None,
                        "qual": 4870.18, "filter": None, "ac": 1, "an": 72302,
                        "ref": "C", "alt": "A"
                        }),
    (__vcf_records[1], {"chrom": "1", "pos": 17415, "id": None,
                        "qual": 7339.66, "filter": "RF;AC0", "ac": 0,
                        "an": 72304, "ref": "G", "alt": "A"
                        }),
    (__vcf_records[2], {"chrom": "1", "pos": 17417, "id": None,
                        "qual": 736.82, "filter": "AC0", "ac": 0, "an": 72310,
                        "ref": "G", "alt": "C"
                        })
]


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


@pytest.mark.parametrize("record, expected", record_dicts_data)
def test_record_dicts(record, expected):
    returned = create_db.vcf_record_as_dict(record)
    ret_non_qual = {k: v for k, v in returned.items() if k != 'qual'}
    exp_non_qual = {k: v for k, v in expected.items() if k != 'qual'}
    assert ret_non_qual == exp_non_qual

    exp_qual = expected['qual']
    returned_qual = returned['qual']
    assert exp_qual*0.99 < returned_qual < exp_qual*1.01  # floats


def test_valueerror_dict(missing_vcf):
    for record in missing_vcf:
        with pytest.raises(ValueError):
            create_db.vcf_record_as_dict(record)


def test_table_creation(temp_db_conn):
    create_db.create_table(temp_db_conn)

    cursor = temp_db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")

    assert cursor.fetchone()[0] == 'variants'


def test_insert_records(inited_conn):
    dicts = list(map(create_db.vcf_record_as_dict, __vcf_records))
    create_db.upsert_record_dicts_to_db(inited_conn, dicts)

    cursor = inited_conn.cursor()
    cursor.execute("SELECT ac, an FROM variants")

    returned = cursor.fetchall()
    assert returned[0] == (1, 72302)
    assert returned[1] == (0, 72304)
    assert returned[2] == (0, 72310)


def test_upsert_records(inited_conn):
    dicts = list(map(create_db.vcf_record_as_dict, __vcf_records))
    create_db.upsert_record_dicts_to_db(inited_conn, dicts)
    create_db.upsert_record_dicts_to_db(inited_conn, dicts)  # do it twice

    cursor = inited_conn.cursor()
    cursor.execute("SELECT ac, an FROM variants")

    returned = cursor.fetchall()
    assert returned[0] == (2, 144604)
    assert returned[1] == (0, 144608)
    assert returned[2] == (0, 144620)


def test_insert_vcf(inited_conn):
    path = mini_vcf_path()
    create_db.upsert_vcf_to_db(inited_conn, path)

    cursor = inited_conn.cursor()
    cursor.execute("SELECT ac, an FROM variants")

    returned = cursor.fetchall()
    assert returned[0] == (1, 72302)
    assert returned[1] == (0, 72304)
    assert returned[2] == (0, 72310)


def test_upsert_vcf(inited_conn):
    path = mini_vcf_path()
    create_db.upsert_vcf_to_db(inited_conn, path)
    create_db.upsert_vcf_to_db(inited_conn, path)

    cursor = inited_conn.cursor()
    cursor.execute("SELECT ac, an FROM variants")

    returned = cursor.fetchall()
    assert returned[0] == (2, 144604)
    assert returned[1] == (0, 144608)
    assert returned[2] == (0, 144620)
