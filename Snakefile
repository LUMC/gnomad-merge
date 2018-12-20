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
Snakemake pipeline to merge gnomad exomes with genomes into one
big VCF containing only global allele frequencies.

:copyright: (c) 2018 Leiden University Medical Center
:copyright: (c) 2018 Sander Bollen
"""
from pathlib import Path

gnomad_data_dir = config.get("GNOMAD_DATA_DIR")
if gnomad_data_dir is None:
    raise ValueError("--config GNOMAD_DATA_DIR must be set")


rule decompose:
    """Decompose VCF files"""
    input: "a vcf"
    output: "a decomposed vcf"
    conda: "envs/vt.yml"
    shell: "vt decompose -s {input} -o {output}"

rule normalize:
    """Normalize VCF files"""
    input:
        vcf="a vcf"
        ref="a fasta"
    output: "a normalized vcf"
    conda: "envs/vt.yml"
    shell: "vt normalize {input.vcf} -r {input.ref} -o {output}"


rule fill_db:
    """File database with variants"""
    input: "collection of vcf files"
    params:
        chunksize="10000"
    output: "db"
    conda: "envs/create_db.yml"
    shell: "python src/create_db.py --chunksize {params.chunksize} -o {output} {input}"


rule export_db
    """Export db back to VCF format"""
    input: "db"
    output: temp("vcf")
    conda: "envs/create_db.yml"
    shell: "python src/db_to_vcf.py {input} > {output}"


rule sort_bgzip
    """Sort and bgzip VCF file"""
    input: "vcf"
    output: "sorted.vcf.gz"
    conda: "envs/sort.yml"
    shell: "bedtools sort -header -i {input} | bgzip -c > {output} && tabix -pvcf {output}"
