# Copyright (C) 2018 Leiden University Medical Center, Sander Bollen
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
db_to_vcf.py
~~~~~~~~~~~~

Export database created with `create_db.py` to VCF format.

:copyright: (c) 2018 Sander Bollen
:copyright: (c) 2018 Leiden University Medical Center

:license: AGPL-3.0-or-later
"""
import argparse
from pathlib import Path
from typing import Tuple, Optional, Iterator
import sqlite3

import tqdm


VCF_HEADER = ('##fileformat=VCFv4.2\n'
              '##FILTER=<ID=PASS,Description="All filters passed">\n'
              '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
              '##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths for the ref and alt alleles in the order listed">\n'  # noqa
              '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">\n'  # noqa
              '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n'  # noqa
              '##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification">\n'  # noqa
              '##FILTER=<ID=RF,Description="Failed random forests filters (SNV cutoff 0.1, indels cutoff 0.2)">\n'  # noqa
              '##FILTER=<ID=AC0,Description="Allele Count is zero (i.e. no high-confidence genotype (GQ >= 20, DP >= 10, AB => 0.2 for het calls))">\n'  # noqa
              '##FILTER=<ID=InbreedingCoeff,Description="InbreedingCoeff < -0.3">\n'  # noqa
              '##FILTER=<ID=LCR,Description="In a low complexity region">\n'
              '##FILTER=<ID=SEGDUP,Description="In a segmental duplication region">\n'  # noqa
              '##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count in genotypes, for each ALT allele, in the same order as listed">\n'  # noqa
              '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency among genotypes, for each ALT allele, in the same order as listed">\n'  # noqa
              '##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles in called genotypes">\n'  # noqa
              '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO')


def create_connection(db_path: Path) -> sqlite3.Connection:
    """Create DB connection"""
    return sqlite3.connect(str(db_path))


def calc_af(ac: int, an: int) -> Optional[float]:
    """Calculate af"""
    if an > 0:
        return ac / an


def db_tuple_to_record(tupl: Tuple) -> str:
    """
    Transform tuple of db row to vcf record as string.
    :param tupl: tuple of values
    :return: vcf row as tab-separated string
    """
    ac = tupl[6]
    an = tupl[7]
    af = calc_af(ac, an)
    info = "AC={0};AF={1};AN={2}".format(
        ac, af if af is not None else ".", an
    )
    rs_id = tupl[2]
    filters = tupl[8]
    row_fmt = "{chrom}\t{pos}\t{id}\t{ref}\t{alt}\t{qual}\t{filter}\t{info}"
    return row_fmt.format(chrom=tupl[0], pos=tupl[1],
                          id=rs_id if rs_id is not None else ".", ref=tupl[3],
                          alt=tupl[4], qual=tupl[5],
                          filter=filters if filters is not None else "PASS",
                          info=info)


def transform_db_to_vcf(db_path: Path) -> Iterator[str]:
    """
    Generate vcf rows from a database
    :param db_path: path to db
    :return: generator of rows as strings.
    """
    conn = create_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM variants")

    for row in cursor:
        yield db_tuple_to_record(row)


def main(db_path: Path) -> None:
    transformer = transform_db_to_vcf(db_path)
    print(VCF_HEADER)
    with tqdm.tqdm(unit="variant", unit_scale=True) as bar:
        for i, row in enumerate(transformer):
            if i % 1000 == 0:
                bar.update(1000)  # only update bar once every 1000 records
            print(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", type=Path, help="Path to db")

    args = parser.parse_args()
    main(args.db_path)
