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