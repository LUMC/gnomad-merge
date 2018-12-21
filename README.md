[![codecov](https://codecov.io/gh/LUMC/gnomad-merge/branch/master/graph/badge.svg)](https://codecov.io/gh/LUMC/gnomad-merge) [![Build Status](https://travis-ci.org/LUMC/gnomad-merge.svg?branch=master)](https://travis-ci.org/LUMC/gnomad-merge)

## Rationale

GnomAD supplies exomes and genomes separately, with genomes in separate
VCF files per contig. If you are interested in global allele frequencies 
you would have to parse all of them. Moreover, the gnomAD VCF files are 
annotated with the VEP for all records. Considering the size of the gnomAD
VCF files,this creates a large parsing performance overhead. If we are only
interested in global allele frequencies, that is time wasted.

Therefore, this repository contains a Snakemake pipeline which combines
the exome and genome VCF files, and emits a single VCF file which only
contains the `AF`, `AN` and `AC` INFO fields. FILTER fields are kept
as is as much as possible.

## Steps

1. Decompose all original VCF files with `vt decompose`
2. Normalize all decomposed vcf files with `vt normalize`
3. Fill sqlite database with AC and AN fields sequentially for all VCF files.
4. Export database to new VCF file, with only AC, AN and AF INFO fields
5. Bgzip and tabix resulting VCF file
6. Optionally convert to BCF. 

## DB structure

One variants table

| Column | Type | Nullable |
| ------ | ---- | -------- |
| CHROM | TEXT | False |
| POS | INT | False | 
| ID | TEXT | True |
| REF | TEXT | False |
| ALT | TEXT | False | 
| QUAL | FLOAT | False | 
| AC | INT | False |
| AN | INT | False |
| FILTER | TEXT | True | 

With primary key on `CHROM + POS + REF + ALT`.

AF is calculated by AC/AN.

If multiple records occur on the same variant the following should happen:

1. QUAL field is updated with the sum of QUAL fields.
2. AC field is updated with the sum of all AC fields.
3. AN field is updated with the sum of all AN fields.
4. ID and FILTER fields are a comma-separated and semicolon-separated set of 
   ID and FILTER fields. # TODO
   
## How to run the pipeline

You need to have a directory containing the gnomad_data file structure that
is generated when downloading the gnomAD VCF fies using the recommended
gsutils option documented [here](http://gnomad.broadinstitute.org/downloads).

This folder structure looks like:

```text
- vcf
--- exomes
------ gnomad.exomes.rrelease.sites.vcf.gz
--- genomes
------ gnomad.genomes.rrelease.sites.1.vcf.gz
------ gnomad.genomes.rrelease.sites.2.vcf.gz
------ <repeat for all chromosomes>
```

Once this directory is in place, you can run the pipeline with:

```bash
snakemake --use-conda -s Snakefile \
--config GNOMAD_DATA_DIR=/path/to/gnomad_data \ 
REFERENCE_FASTA=/path/to/ref.fasta 
```

It is highly recommended to use the provided conda environments!

### Configuration

We've already seen in the previous section that we used two configuration
values. The full list of configuration options is outlined in the table below.

| configuration key | description | required | default (if any) |
| ----------------- | ----------- | -------- | ---------------- |
| GNOMAD_DATA_DIR | Path to gnomad_data directory | Yes | NA | 
| REFERENCE_FASTA | reference file to be used for variant normalization | Yes | NA |
| CHUNKSIZE | Amount of variants added to db in one chunk. Increase to speed up | No | 1000 |
| EXCLUDE_PATTERNS | Comma-separated list of filename patterns to exclude in collection of VCF files to be used as inputs for pipeline. GnomAD supplies "coding" files as part of the genome collection. Not excluding those would result in doubly added variants to the database. | No | `coding` |