# Copyright (C) 2018 Leiden University Medical Center
# This file is part of gnomad-merge
#
# pytest-workflow is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# pytest-workflow is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with gnomad-merge.  If not, see <https://www.gnu.org/licenses/
"""
create_db.py
~~~~~~~~~~~~

Create database based on a set of genome VCF files and one exome VCF file.

This will first insert all records in the genome VCF files, which are
assumed to contain the vast majority fo records. After that, it will upsert
the exome records.

:copyright: (c) 2018 Sander Bollen
:copyright: (c) 2018 Leiden University Medical Center

:license: AGPL-3.0
"""
import argparse
from pathlib import Path
import cyvcf2
import sqlite3
from typing import Dict, List, Iterator


_variants_insert_fmt = ("INSERT INTO variants VALUES (:chrom, :pos, :id, "
                        ":ref, :alt, :qual, :ac, :an, :filter)")


def vcf_record_as_dict(record: cyvcf2.Variant) -> Dict:
    """
    Transform vcf record to a dict
    :param record: cyvcf2 variant record
    :return: dict with keys (chrom, pos, ref, alt, qual, ac, an, filter)
    :raises: ValueError in case AC or AN is not defined for record.
    """
    alt = record.ALT[0]  # we do not support multiallelic variants
    try:
        ac = record.INFO['AC']
        an = record.INFO['AN']
    except KeyError:
        raise ValueError("Record at {0}:{1} does not have AC or AN "
                         "fields".format(record.CHROM, record.POS))
    return {
        "chrom": record.CHROM,
        "pos": record.POS,
        "id": record.ID,
        "ref": record.REF,
        "alt": alt,
        "qual": record.QUAL,
        "ac": ac,
        "an": an,
        "filter": record.FILTER
    }


def insert_record_dicts_to_db(conn: sqlite3.Connection,
                              dicts: List[Dict]) -> None:
    """
    Insert a collection of record dictionaries to the database.
    :param conn: connection to database
    :param dicts: list of dictionaries to insert
    :return: None
    :raises sqlite.Error
    """
    cursor = conn.cursor()

    try:
        cursor.executemany(_variants_insert_fmt, dicts)
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise e


def generate_chunks(vcf: cyvcf2.VCFReader,
                    chunksize: int) -> Iterator[List[cyvcf2.Variant]]:
    """Generate chunks of variant records for a vcf reader"""
    chunk = []
    for record in vcf:
        if len(chunk) == chunksize:
            yield chunk
            chunk = []  # reset
        chunk.append(record)
    yield chunk


def insert_vcf_to_db(conn: sqlite3.Connection, vcf_path: Path,
                     chunksize: int = 1000) -> None:
    """
    Insert all records in a VCF file to the db
    :param conn: connection to database
    :param vcf_path: path to vcf file
    :param chunksize: amount of vcf records to insert simultaneously.
    :return: none
    """
    reader = cyvcf2.VCF(str(vcf_path))
    chunker = generate_chunks(reader, chunksize)
    for i, chunk in enumerate(chunker):
        dicts = list(map(vcf_record_as_dict, chunk))
        insert_record_dicts_to_db(conn, dicts)


def create_connection(db_path: Path) -> sqlite3.Connection:
    """Create DB connection"""
    return sqlite3.connect(str(db_path))


def create_table(conn: sqlite3.Connection) -> None:
    """
    Create Variants table if it doesn't yet exist
    :param conn: database connection
    :return:
    """
    variant_table_str = ("CREATE TABLE IF NOT EXISTS variants "
                         "(chrom TEXT NOT NULL, "
                         "pos INTEGER NOT NULL, "
                         "id TEXT, "
                         "ref TEXT NOT NULL, "
                         "alt TEXT NOT NULL, "
                         "qual REAL NOT NULL, "
                         "ac INTEGER NOT NULL, "
                         "an INTEGER  NOT NULL, "
                         "filter TEXT, "
                         "PRIMARY KEY (chrom, pos, ref, alt))")
    cursor = conn.cursor()
    cursor.execute(variant_table_str)
    conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--exome-vcf", required=True, type=Path,
                        help="Path to exome VCF")
    parser.add_argument("-o", "--output", required=True, type=Path,
                        help="Path to output database file")
    parser.add_argument("genome_vcfs", type=Path, nargs="+",
                        help="List of genome VCFs")

    args = parser.parse_args()

    conn = create_connection(args.output)
    create_table(conn)

