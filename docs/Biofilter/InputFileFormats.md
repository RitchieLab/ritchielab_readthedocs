# SNP List Input
SNP input files only require one column listing the RS number of each SNP, which may optionally begin with the “`rs`” prefix. If all inputs and outputs only deal with SNPs, then these RS numbers will all be used as-is. If any additional columns are included, they will be stored and returned via the “`snp_extra`” output column.

If any part of the analysis involves any other data types, however, then the provided RS numbers will have to be mapped to positions using the prior knowledge database. In this case a single RS number may correspond to multiple genomic positions, or it may have no known position (at least on the current genomic reference build). For these reasons it may be preferable to provide positions directly, if available, rather than relying on SNP identifiers.

Example:
```
#snp        extra
rs123       first snp
456         second snp
rs789       third snp
```

# Position Data Input
The input file format for position data is similar to the `MAP` file format used in [PLINK](https://www.cog-genomics.org/plink/2.0/formats#map). Up to four primary data columns are allowed, separated by tab characters:
* Chromosome (1-22, X, Y, MT)
* RS number or other label
* Genetic distance (ignored by Biofilter)
* Base pair position

If all four of these columns are provided, then any additional columns will be stored and returned via the “`position_extra`” output column.

Since the genetic distance column is not used by Biofilter, it may be omitted entirely for a three-column format (equivalent to PLINK’s `--map3` option). The label column may also be omitted for a two-column format including only the chromosome and position; in this case a label of the form “`chr1:2345`” will be automatically generated. Note that if the label column is used, it does not necessarily have to be a known SNP’s RS number; whatever arbitrary label is provided will be used by Biofilter to refer to the position whenever it appears in any output file.

Example:
```
#chr    label       distance    pos         extra
7       rs123       -           24966446    first position
7       rs456       -           24962419    second position
3       rs789       -           29397015    third position
```

# Region Data Input
The file format for region input data is similar to that of positional data. Up to four primary data columns are allowed, separated by tab characters:
* Chromosome (1-22, X, Y, MT)
* Gene symbol or other label
* Base pair start position
* Base pair stop position

If all four of these columns are provided, then any additional columns will be stored and returned via the “`region_extra`” output column.

As with positional data, the label column does not necessarily have to be a known gene symbol, and can be omitted entirely. If the column is omitted then a label of the form “`chr1:2345-6789`” will be generated automatically; if labels are provided, then Biofilter will use them to refer to the regions whenever they appear in any output file.

Example:
```
#chr    label       start       stop        extra
7       THSD7A      11410061    11871823    first region
7       OSBPL3      24836158    25019759    second region
3       RBMS3       29322802    30051885    third region
```

# Gene and Group List Input
Like the SNP input file format, a gene or group input file may simply be a single column of identifiers. Unlike the SNP file format, gene or group input files may alternatively include two columns separated by a tab character; in this case, the first column lists the type of the identifier which is in the second column on the same line, and any additional columns after these two will be stored and returned via the “`gene_extra`” or “`group_extra`” output columns.

The `--gene-identifier-type` and `--group-identifier-type` options specify the default type for any user-provided gene or group identifiers, respectively. This applies to any identifiers given directly via the `--gene` or `--group` options, and any identifiers listed in single-column gene or group list input files. These options do not apply to two-column gene or group input files, since those files specify their own identifier types in the first column.

An empty identifier type (a blank in the first column of a two-column gene input file, or a `--gene-identifier-type`/`--group-identifier-type` option with no argument) causes Biofilter to attempt to interpret the identifier using any known type. The special identifier type “`-`” instead causes Biofilter to interpret identifiers as primary labels of genes or groups, and the special type “`=`” accepts the `gene_id` or `group_id` output values from a previous Biofilter run.

It is important to recall that gene and group identifiers can vary in their degree of uniqueness. For analyses that depend on a gene’s genomic region (such as comparisons with SNPs or other positions) it may be preferable to provide the regions directly rather than relying on gene identifiers. If a single identifier matches more than one gene or group, Biofilter will ignore it unless the appropriate `--allow-ambiguous-genes` or `--allow-ambiguous-groups` option is used.

Examples:
```
#gene
THSD7A
OSBPL3
RBMS3
```
```
#namespace          name                extra
symbol              THSD7A              first gene
entrez_gid          26031               second gene
ensemble_gid        ENSG00000144642     third gene
```

# Source List Input
Since the knowledge sources in [LOKI](https://ritchielab.github.io/biofilter-manual/loki/loki.html) all have single, unique names, there are no identifier types to consider. Source input files simply contain a single column with the name of a source on each line.

Note that sources play a slightly different role in Biofilter than in LOKI. When building the prior knowledge database, every source is relevant because they all contribute a different set of knowledge to the final product: many sources provide groupings of genes or proteins (pathways, interactions, etc), while others provide information about genes or SNPs themselves (such as their regions or boundaries, alternate names, etc). In Biofilter, however, sources are only considered in connection with groups; providing a source list to focus a Biofilter analysis is therefore exactly the same as providing a group list which includes every group from the source(s) in the source list. In particular, the sources which LOKI used to define basic SNP and gene information (such as “dbsnp” or “entrez”) are not relevant to Biofilter since those sources generally do not define any groupings of genes; consequently, using any of those sources as inputs to Biofilter will generally result in no output.

Example:
```
#source
netpath
```

# User-Defined Knowledge Input
In addition to the gene groupings which are compiled into [LOKI](https://ritchielab.github.io/biofilter-manual/loki/loki.html) from each external source, you may also specify your own custom gene groupings for use in your specific analysis. These custom groups are provided in a user-defined knowledge input file, which begins with a single header line identifying the user-defined source as a whole, followed by any number of sections which each describe one gene group within the source.

The source header line must begin with a single word which serves as the source’s shorthand label, analogous to “biogrid”, “netpath”, etc. This is the label that will be output in the “`source`” column of any results which involve this user-defined source, and which may be used as a source filter. Any additional words on this source header line will be stored as a longer description of the source.

Each group section must begin with a section header line. The section header line must begin with the word “`GROUP`”, followed by a single word which serves as the group’s shorthand label. Any additional words on this section header line will be stored as a longer description of the group.

Each line after a group section header, and before the next group section (identified by a line beginning with the word “GROUP”), may contain any number of gene identifiers which will be assigned to this group. These gene identifiers will be interpreted according to the `--gene-identifier-type` option.

Example:
```
cancer My custom cancer gene sets
GROUP lung My lung cancer genes
A1BC
D2EF
GROUP liver My liver cancer genes
G3HI
J4KL
M5NO
```