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
reference_fasta = config.get("REFERENCE_FASTA")
chunksize = config.get("CHUNKSIZE", 1000)
if gnomad_data_dir is None:
    raise ValueError("--config GNOMAD_DATA_DIR must be set")
if reference_fasta is None:
    raise ValueError("--config REFERENCE_FASTA must be set")


# gnomad_data folder structure looks like:
# - vcf
# --- exomes
# ------ gnomad.exomes.r2.0.1.sites.vcf.gz
# --- genomes
# ------ gnomad.genomes.r2.0.1.sites.1.vcf.gz
# ------ gnomad.genomes.r2.0.1.sites.2.vcf.gz


vcf_dir = Path(gnomad_data_dir) / Path("vcf")
exome_dir = vcf_dir / Path("exomes")
genome_dir = vcf_dir / Path("genomes")

# get some sensible name for a file
# e.g. exomes.r2.0.1.sites
# so it can be used as a wildcard
def get_name_for_file(path: Path):
    return path.name.split(".vcf.gz")[0].split("gnomad.")[-1]

# generate dictionary of names -> absolute paths
all_input_vcf_files = {}
for x in exome_dir.iterdir():
    if x.name.endswith(".vcf.gz"):
        all_input_vcf_files[get_name_for_file(x)] = x.absolute()
for g in genome_dir.iterdir():
    if g.name.endswith(".vcf.gz"):
        all_input_vcf_files[get_name_for_file(g)] = g.absolute()


def get_file_for_name(wilcards):
    """Input function to get absolute path pertaining to a file"""
    return str(all_input_vcf_files[wilcards.fname])


rule all: "exports/gnomad.all.vcf.gz"


rule decompose:
    """Decompose VCF files"""
    input: get_name_for_file
    output: temp("decomposed/{fname}.vcf")
    conda: "envs/vt.yml"
    shell: "vt decompose -s {input} -o {output}"

rule normalize:
    """Normalize VCF files"""
    input:
        vcf="decomposed/{fname}.vcf",
        ref=reference_fasta
    output: temp("normalized/{fname}.vcf")
    conda: "envs/vt.yml"
    shell: "vt normalize {input.vcf} -r {input.ref} -o {output}"


rule fill_db:
    """File database with variants"""
    input: expand("normalized/{fname}.vcf", fname=list(all_input_vcf_files.keys()))
    params:
        chunksize=chunksize
    output: "db/gnomad.db"
    conda: "envs/create_db.yml"
    shell: "python src/create_db.py --chunksize {params.chunksize} -o {output} {input}"


rule export_db:
    """Export db back to VCF format"""
    input: "db/gnomad.db"
    output: temp("exports/unsorted.vcf")
    conda: "envs/create_db.yml"
    shell: "python src/db_to_vcf.py {input} > {output}"


rule sort_bgzip:
    """Sort and bgzip VCF file"""
    input: "exports/unsorted.vcf"
    output: "exports/gnomad.all.vcf.gz"
    conda: "envs/sort.yml"
    shell: "bedtools sort -header -i {input} | bgzip -c > {output} && tabix -pvcf {output}"
