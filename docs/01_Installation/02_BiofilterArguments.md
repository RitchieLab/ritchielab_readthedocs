# Using Biofilter
### Biofilter from the Command Line

Biofilter can be run from a command-line terminal by executing “biofilter.py” (or “python3 biofilter.py”) and specifying the desired inputs, outputs and other optional settings. 
    ```
    python3 biofilter.py [options]
    ```
There are two main ways to provide inputs and configure settings:

1. **Directly on the command line:**
    - This is useful for quick tests or small-scale analyses.

2. **Using configuration files:**
    - Recommended for final runs, as these files serve as a record of exactly what was done and can be reused easily.

## Command Line Options vs Configuration File Options
Any number of configuration files may be used, with options from later files overriding those from earlier files. Options on the command line override those from any configuration file.

The available options are the same no matter where they appear, but are formatted differently. 
**Command Line:** Options are lowercase, start with two dashes `--`, and use single dashes `-` to separate words.

- Example: `--snp-file or -s`

**Configuration File:** Options are uppercase, contain no dashes, and use underscores `_` to separate words.

- Example: `SNP_FILE`

## Example 1: Running Biofilter via Command Line
Here is an example of running Biofilter with options directly from the command line:
```
python3 biofilter.py --snp-file snp_data.txt --allow-ambiguous-genes
```
In this case, the snp-file is provided with the `--snp-file option`, and ambiguous genes are allowed by adding the `--allow-ambiguous-genes flag`.

## Example 2: Using a Configuration File
You can place all options in a configuration file, which is ideal for saving your settings:
`analysis.config` **(Configuration File):**
```
SNP_FILE=snp_data.txt
ALLOW_AMBIGUOUS_GENES=1
```
You would run Biofilter using this configuration file with the following command:
```
python3 biofilter.py analysis.config
```
### Combining Command Line and Configuration Files
You can also combine both methods by overriding settings in the configuration file from the command line. For example, if your configuration file has a different SNP file but you want to override it for a single run, you can specify a new SNP file on the command line:
```
python3 biofilter.py analysis.config --snp-file new_snp_data.txt
```
### Option Shorthands
For convenience, many command line options also have shorthand versions:

1. Long-form: `--snp-file`

2. Short-form: `-s`

**Example Using Shorthand Options:**
```
python3 biofilter.py -s snp_data.txt -aag
```
Here, the shorthand `-s` is used for `--snp-file`, and `-aag` is used for `--allow-ambiguous-genes`.

All options are listed here in both their command line and configuration file forms. If an option allows or requires any further arguments, they are also noted along with their default values, if any. Arguments which are required are enclosed in \<angle brackets\>, while arguments which are optional are enclosed in [square brackets].

Many options have only two possible settings and therefore accept a single argument which can either be “yes” or “no” (or “on” or “off”, or “1” or “0”). Specifying these options with no argument is always interpreted as a “yes”, such that for example “VERBOSE yes” and “VERBOSE” have the same meaning. However, omitting such options entirely may default to either “yes” or “no” depending on the option.

# Analysis Options:

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--filter` | \[ \[TYPE1] \[TYPE2] \[...] ] | Default: *NONE*. Perform a filtering analysis which outputs the specified type(s). If a single type is requested, the output will be in exactly the same format that Biofilter requires as input for that data type; additional types are simply appended left-to-right in the order requested. |
| `--annotate` | \[ \[TYPE1] \[TYPE2] \[...] \[:] \[TYPE1] \[TYPE2] ] | Default: *NONE*. Perform an annotation analysis which outputs the specified type(s). The starting point for the annotation is the first specified type (or, if a colon is used, the combination of types before the colon); all additional types are optional and will be left blank if no suitable match can be found. |
| `--model` | \[ \[TYPE1] \[TYPE2] \[...] \[:] \[TYPE1] \[TYPE2] ] | Default: *NONE*. Perform a modeling analysis which generates models of the specified type(s). If a colon is used, the types before and after the colon will appear on the left and right sides of the generated models, respectively; with no colon, both sides of the models will have the same type(s). |
| `--paris` | [yes \| **no**] | Default: **no**. Performs PARIS analysis using the provided SNP or position association test results and LD block regions. See the PARIS Appendix for more information. TODO: what is this and where to link to? |

# Types for Analysis Option Arguments:

| **types** | **Information** |
|---|---|
|user_input|Provided user input outputed as first column by default; snp_label, position_label, gene_label, region_label, group_label, or source_label
|snp|Shorthand for: snp_label
|snp_id|The SNP’s RS number, with no prefix; if an input SNP was merged, the current (new) RS number is shown
|snp_label|The SNP’s RS number, with “rs” prefix; if an input SNP was merged, the user-provided (old) RS number is shown
|snp_extra|Any extra columns provided in the SNP input file
|position|Shorthand for: position_chr , position_label , position_pos
|position_id|An arbitrary unique ID number for the position; can be used to distinguish unlabeled positions with identical genomic locations
|position_label|The provided (or generated) label for an input position, or the RS number (with “rs” prefix) for a SNP position from the knowledge database
|position_chr|The position’s chromosome number or name
|position_pos|The position’s basepair location
|position_extra|Any extra columns provided in the position input file
|region|Shorthand for: region_chr , region_label , region_start , region_stop
|region_id|An arbitrary unique ID number for the region; can be used to distinguish unlabeled regions with identical genomic start and stop locations
|region_label|The provided (or generated) label for an input region, or the primary label for a region from the knowledge database
|region_chr|The region’s chromosome number or name
|region_start|The region’s basepair start location
|region_stop|The region’s basepair stop location
|region_extra|Any extra columns provided in the region input file 
|generegion|Shorthand for: region_chr , gene_label , region_start , region_stop Similar to “region” except that only gene regions from the knowledge database are returned, even if the user also provided input regions
|gene|Shorthand for: gene_label
|gene_id|An arbitrary unique ID number for the gene; can be used to distinguish genes with identical labels
|gene_label|The provided identifier for an input gene, or the primary label for a gene from the knowledge database
|gene_description|The gene’s descriptive text from the knowledge database, if any
|gene_identifiers|All known identifiers for the gene, of any type; formatted as “type:name\|type:name\|…”
|gene_symbols|All known “symbol”-type identifiers (symbolic aliases) for the gene, formatted as “symbol\|symbol\|…”
|gene_extra|Any extra columns provided in the gene input file
|upstream|Shorthand for: upstream_label , upstream_distance
|upstream_id|An arbitrary unique ID number for the closest upstream gene
|upstream_label|The primary label for the closest upstream gene
|upsteam_distance|The distance to the closest upstream gene
|upsteam_start|The closest upstream gene’s basepair start location
|upsteam_stop|The closest upstream gene’s basepair stop location
|downstream|Shorthand for: downstream_label , downstream_distance
|downstream_id|An arbitrary unique ID number for the closest downstream gene
|downstream_label|The primary label for the closest downstream gene
|downstream_distance|The distance to the closest downstream gene
|downstream_start|The closest downstream gene’s basepair start location
|downstream_stop|The closest downstream gene’s basepair stop location
|group|Shorthand for: group_label
|group_id|An arbitrary unique ID number for the group; can be used to distinguish groups with identical labels
|group_label|The provided identifier for an input group, or the primary label for a group from the knowledge database
|group_description|The group’s descriptive text from the knowledge database, if any
|group_identifiers|All known identifiers for the group, of any type; formatted as \|“type:name\|type:name\|…”
|group_extra|Any extra columns provided in the group input file
|source|Shorthand for: source_label
|source_id|An arbitrary unique ID number for the source; included for completeness
|source_label|The source’s name
|gwas|Shorthand for: gwas_trait , gwas_snps , gwas_orbeta , gwas_allele95ci , gwas_riskAfreq , gwas_pubmed
|gwas_rs|The RS# which led to the GWAS annotation match
|gwas_chr|The chromosome on which the GWAS match was found
|gwas_pos|The basepair location at which the GWAS match was found
|gwas_trait|The GWAS annotation’s associated trait or phenotype
|gwas_snps|The full list of SNPs in the GWAS association
|gwas_orbeta|The odds ratio or beta of the GWAS association
|gwas_allele95ci|The allele 95% confidence interval of the GWAS association
|gwas_riskAfreq|The risk allele frequency of the GWAS association
|gwas_pubmed|The PubMedID of the GWAS association
|disease|Shorthand for: disease_label and disease_category
|disease_label|The provided label for the disease
|disease_category|The provided category of the disease

# Configuration Options

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--help` |  | Displays the program usage and immediately exits. |
| `--version` |  | Displays the software versions and immediately exits. (Note that Biofilter is built upon LOKI and SQLite, each of which will also report their own software versions.) |
| `--report-configuration` | [yes \| **no**] | Default: **no**. Generates a Biofilter configuration file which specifies the current effective value of all program options, including any default options which were not overridden. This file can then be passed back in to Biofilter again in order to repeat exactly the same analysis. |
| `--report-replication-fingerprint` | [yes \| **no**] | Default: **no**. When used along with `--report-configuration`, this adds additional validation options to the resulting configuration file. These extra options specify all relevant software versions as well as a fingerprint of the data contained in the knowledge database file. When re-running a configuration file with these extra replication options, Biofilter will use them to ensure that neither Biofilter itself nor the LOKI knowledge database file have been updated since the original analysis; this in turn ensures that the re-run analysis will produce the same (or compatible) results as the original. |
| `--random-number-generator-seed` | \[SEED] | Default: *NONE* (no seed is provided). Sets the random number generator seed value. For modes which involve randomization (such as PARIS), the output will be different every time unless the seed value is kept consistent. If left blank (the default), the seed value itself is randomized on each execution of the program. |


# Prior Knowledge Options

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--knowledge` | \[FILE] | Default: *NONE*. Specifies the LOKI prior knowledge database file to use. If a relative path is provided it will be tried first from the current working directory, and then from the location of the Biofilter executable itself. |
| `--report-genome-build` | [**yes** \| no] | Default: **yes**. Displays the build version(s) of the human reference genome which was used as the basis for all genomic positions in the prior knowledge database (such as for SNP positions and gene regions). Any position or region data provided as input must be converted to the same build version in order to match correctly with the prior knowledge. |
| `--report-gene-name-stats` | [yes \| **no**] | Default: **no**. Generates a report of the gene identifier types available in the knowledge database. |
| `--report-group-name-stats` | [yes \| **no**] | Default: **no**. Generates a report of the group identifier types available in the knowledge database. |
| `--allow-unvalidated-snp-positions` | [**yes** \| no] | Default: **yes**. Allows Biofilter to make use of all SNP-position mappings available in the knowledge database, even ones which the original data source identified as un-validated. When disabled, only validated positions are considered. |
| `--allow-ambiguous-snps` | [yes \| **no**] | Default: **no**. Allows Biofilter to make use of all SNP-position mappings available in the knowledge database, even when the same SNP is believed to have multiple positions. When disabled, only SNPs with single, unambiguous positions are considered. |
| \*\*\* `--allow-ambiguous-knowledge` | [yes \| **no**] | Default: **no**. Allows Biofilter to make use of all potential gene-group mappings in the knowledge database, even if the gene was referred to with an ambiguous identifier. This will likely include some false-positive associations, but the alternative is likely to miss some true associations. (TODO: is this still gene-group?) |
| \*\*\* `--reduce-ambiguous-knowledge` | [**no** \| implication \| quality \| any] | Default: **no**. Enables a heuristic algorithm to attempt to resolve ambiguous gene-group mappings in the knowledge database. This option’s argument is optional and defaults to ‘any’, which applies all heuristics together. (TODO: is the default 'no' or 'any'?) |
| \*\*\* `--report-ld-profiles` | [yes \| **no**] | Default: **no**. Generates a report of the LD profiles available in the knowledge database. See Appendix 2 for details. (TODO: refers to Appendix 2, but where will that be placed?) |
| `--ld-profile` | \[PROFILE] | Default: *NONE*. Specifies an alternate set of gene region boundaries which were pre-calculated by LD Spline to account for a population-specific linkage disequilibrium profile. When omitted or supplied with no argument, the default profile (containing the original unmodified gene boundaries) is used. |
| `--verify-biofilter-version` | \[VERSION] | Default: *NONE*. Ensure that the current version of Biofilter is the same as the one specified. This option is added automatically to configuration files generated with `--report-replication-fingerprint`. |
| `--verify-loki-version` | \[VERSION] | Default: *NONE*. Ensure that the current version of LOKI is the same as the one specified. This option is added automatically to configuration files generated with `--report-replication-fingerprint`. |
| `--verify-source-loader` | \[ \[SOURCE] VERSION ] | Default: *NONE*. Ensure that the knowledge database file was generated with the specified version of a source data loader module. Can be used multiple times to specify versions for different sources. This option is added automatically to configuration files generated with `--report-replication-fingerprint`. |
| `--verify-source-option` | \[ \[SOURCE] VERSION VALUE ] | Default: *NONE*. Ensure that the knowledge database file was generated with the specified option value supplied to a source data loader module. Can be used multiple times to specify different options, or options for different sources. This option is added automatically to configuration files generated with `--report-replication-fingerprint`. |
| `--verify-source-file` | \[ \[SOURCE] FILE DATE SIZE MD5 ] | Default: *NONE*. Ensure that the knowledge database file was generated with the specified source data file. Can be used multiple times to specify different files, or files for different sources. This option is added automatically to configuration files generated with`--report-replication-fingerprint`. |
| `--user-defined-knowledge` | \[FILE] | Default: *NONE*. Adds a user-defined knowledge source (one per file) containing any number of user-defined pathways or other gene groupings. |
| `--user-defined-filter` | \[**no** \| group \| gene] | Default: **no**. Applies the user-defined knowledge as if it were a group filter (analogous to the `--group` or `--group-file` options) or gene filter (analogous to the `--gene` or `--gene-file` options). As a group filter, all groups which contain at least one gene which is also in a user-defined group will be loaded; as a gene filter, all genes which are in any user-defined group will be loaded. |


# Input Data Options (Primary)

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--snp` | \[ \[SNP1] \[SNP2] \[...] ] | Default: *NONE*. An arbitrary number of SNPs may be listed after this command. Adds (or intersects) the specified set of SNPs to (or with) the primary input dataset. SNPs must be provided as integer RS (reference SNP) numbers with an optional “rs” prefix. |
| `--snp-file` | \[ \[SNP_FILE1] \[SNP_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of SNPs read from the specified files to (or with) the primary input dataset. Files must contain a single column with entries formatted as described in the `--snp` option. |
| `--position` | \[ \[POSITION1] \[POSITION2] \[...] ] | Default: *NONE*. An arbitrary number of positions may be listed after this command. Adds (or intersects) the specified set of positions to (or with) the primary input dataset. Positions must be provided as 2 to 4 fields separated by colons: “chr:pos”, “chr:label:pos” or “chr:label:ignored:pos”. Chromosomes may have an optional “chr” prefix. |
| `--position-file` | \[ \[POSITION_FILE1] \[POSITION_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of positions read from the specified files to (or with) the primary input dataset. Files must contain 2 to 4 columns formatted as in the `--position` option, but separated by tabs instead of colons. |
| `--region` | \[ \[REGION1] \[REGION2] \[...] ] | Default: *NONE*. An arbitrary number of regions may be listed after this command. Adds (or intersects) the specified set of regions to (or with) the primary input dataset. Regions must be provided as 3 or 4 fields separated by colons: “chr:start:stop” or “chr:label:start:stop”. Chromosomes may have an optional “chr” prefix. |
| `--region-file` | \[ \[REGION_FILE1] \[REGION_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of regions read from the specified files to (or with) the primary input dataset. Files must contain 3 to 4 columns formatted as in the `--region` option, but separated by tabs instead of colons. |
| `--gene` | \[ \[GENE1] \[GENE2] \[...] ] | Default: *NONE*. An arbitrary number of genes may be listed after this command. Adds (or intersects) the specified set of genes to (or with) the primary input dataset. Genes will be interpreted according to the `--gene-identifier-type` option. |
| `--gene-file` | \[ \[GENE_FILE1] \[GENE_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of genes read from the specified files to (or with) the primary input dataset. Files must contain 1 or 2 columns separated by tabs. For 1-column files, all entries are interpreted as genes according to the `--gene-identifier-type` option. For 2-column files, the first column specifies the gene identifier type by which the second column will be interpreted. |
| `--gene-identifier-type` | \[TYPE \| **-**] | Default: *-*. Specifies the identifier type with which to interpret all input gene identifiers. If no type or an empty type is provided, all possible types are tried for each identifier. If the special type “-“ is provided (the default), identifiers are interpreted as primary gene labels. |
| `--allow-ambiguous-genes` | \[yes \| **no**] | Default: **no**. When enabled, any input gene identifier which matches multiple genes will be interpreted as if all of those genes had been specified. When disabled (the default), ambiguous gene identifiers are ignored. |
| `--gene-search` | \[TEXT] | Default: *NONE*. Adds (or intersects) the matching set of genes to (or with) the primary input dataset. Matching genes are identified by searching for the provided text in all labels, descriptions and identifiers associated with each known gene. |
| `--group` | \[ \[GROUP1] \[GROUP2] \[...] ] | Default: *NONE*. An arbitrary number of groups may be listed after this command. Adds (or intersects) the specified set of genes to (or with) the primary input dataset. Genes will be interpreted according to the `--group-identifier-type` option. |
| `--group-file` | \[ \[GROUP_FILE1] \[GROUP_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of groups read from the specified files to (or with) the primary input dataset. Files must contain 1 or 2 columns separated by tabs. For 1-column files, all entries are interpreted as genes according to the `--group-identifier-type` option. For 2-column files, the first column specifies the group identifier type by which the second column will be interpreted. |
| `--group-identifier-type` | \[TYPE \| **-**] | Default: *-*. Specifies the identifier type with which to interpret all input group identifiers. If no type or an empty type is provided, all possible types are tried for each identifier. If the special type “-“ is provided (the default), identifiers are interpreted as primary group labels. |
| `--allow-ambiguous-groups` | \[yes \| **no**] | Default: **no**. When enabled, any input group identifier which matches multiple groups will be interpreted as if all of those groups had been specified. When disabled (the default), ambiguous group identifiers are ignored. |
| `--group-search` | \[TEXT] | Default: *NONE*. Adds (or intersects) the matching set of groups to (or with) the primary input dataset. Matching groups are identified by searching for the provided text in all labels, descriptions and identifiers associated with each known group. |
| `--source` | \[ \[SOURCE1] \[SOURCE2] \[...] ] | Default: *NONE*. An arbitrary number of sources may be listed after this command. Adds (or intersects) the specified set of sources to (or with) the primary input dataset. |
| `--source-file` | \[ \[SOURCE_FILE1] \[SOURCE_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of sources read from the specified files to (or with) the primary input dataset. |


# Input Data Options (Alternative)

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--alt-snp` | \[ \[SNP1] \[SNP2] \[...] ] | Default: *NONE*. An arbitrary number of SNPs may be listed after this command. Adds (or intersects) the specified set of SNPs to (or with) the alternate input dataset. SNPs must be provided as integer RS (reference SNP) numbers with an optional “rs” prefix. |
| `--alt-snp-file` | \[ \[SNP_FILE1] \[SNP_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of SNPs read from the specified files to (or with) the alternate input dataset. Files must contain a single column with entries formatted as described in the `--snp` option. |
| `--alt-position` | \[ \[POSITION1] \[POSITION2] \[...] ] | Default: *NONE*. An arbitrary number of positions may be listed after this command. Adds (or intersects) the specified set of positions to (or with) the alternate input dataset. Positions must be provided as 2 to 4 fields separated by colons: “chr:pos”, “chr:label:pos” or “chr:label:ignored:pos”. Chromosomes may have an optional “chr” prefix. |
| `--alt-position-file` | \[ \[POSITION_FILE1] \[POSITION_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of positions read from the specified files to (or with) the alternate input dataset. Files must contain 2 to 4 columns formatted as in the `--position` option, but separated by tabs instead of colons. |
| `--alt-region` | \[ \[REGION1] \[REGION2] \[...] ] | Default: *NONE*. An arbitrary number of regions may be listed after this command. Adds (or intersects) the specified set of regions to (or with) the alternate input dataset. Regions must be provided as 3 or 4 fields separated by colons: “chr:start:stop” or “chr:label:start:stop”. Chromosomes may have an optional “chr” prefix. |
| `--alt-region-file` | \[ \[REGION_FILE1] \[REGION_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of regions read from the specified files to (or with) the alternate input dataset. Files must contain 3 to 4 columns formatted as in the `--region` option, but separated by tabs instead of colons. |
| `--alt-gene` | \[ \[GENE1] \[GENE2] \[...] ] | Default: *NONE*. An arbitrary number of genes may be listed after this command. Adds (or intersects) the specified set of genes to (or with) the alternate input dataset. Genes will be interpreted according to the `--gene-identifier-type` option. |
| `--alt-gene-file` | \[ \[GENE_FILE1] \[GENE_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of genes read from the specified files to (or with) the alternate input dataset. Files must contain 1 or 2 columns separated by tabs. For 1-column files, all entries are interpreted as genes according to the `--gene-identifier-type` option. For 2-column files, the first column specifies the gene identifier type by which the second column will be interpreted. |
| `--alt-gene-search` | \[TEXT] | Default: *NONE*. Adds (or intersects) the matching set of genes to (or with) the alternate input dataset. Matching genes are identified by searching for the provided text in all labels, descriptions and identifiers associated with each known gene. |
| `--alt-group` | \[ \[GROUP1] \[GROUP2] \[...] ] | Default: *NONE*. An arbitrary number of groups may be listed after this command. Adds (or intersects) the specified set of genes to (or with) the alternate input dataset. Genes will be interpreted according to the `--group-identifier-type` option. |
| `--alt-group-file` | \[ \[GROUP_FILE1] \[GROUP_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of groups read from the specified files to (or with) the alternate input dataset. Files must contain 1 or 2 columns separated by tabs. For 1-column files, all entries are interpreted as genes according to the `--group-identifier-type` option. For 2-column files, the first column specifies the group identifier type by which the second column will be interpreted. |
| `--alt-group-search` | \[TEXT] | Default: *NONE*. Adds (or intersects) the matching set of groups to (or with) the alternate input dataset. Matching groups are identified by searching for the provided text in all labels, descriptions and identifiers associated with each known group. |
| `--alt-source` | \[ \[SOURCE1] \[SOURCE2] \[...] ] | Default: *NONE*. An arbitrary number of sources may be listed after this command. Adds (or intersects) the specified set of sources to (or with) the alternate input dataset. |
| `--alt-source-file` | \[ \[SOURCE_FILE1] \[SOURCE_FILE2] \[...] ] | Default: *NONE*. An arbitrary number of files may be listed after this command. Adds (or intersects) the set of sources read from the specified files to (or with) the alternate input dataset. |


# Positional Matching Options

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--grch-build-version` | \[VERSION] | Default: *NONE*. Specifies the GRCh# reference genome build version of input positions or regions data. If this build version differs from the build version of the LOKI knowledge database, the input data will have liftOver applied automatically to make it compatible with LOKI. If no build is specified, input data is assumed to already be on the same build version as LOKI. |
| `--ucsc-build-version` | \[VERSION] | Default: *NONE*. Specifies the UCSC hg# reference genome build version of input positions or regions data. If this build version differs from the build version of the LOKI knowledge database, the input data will have liftOver applied automatically to make it compatible with LOKI. If no build is specified, input data is assumed to already be on the same build version as LOKI. |
| `--coordinate-base` | \[\<INT OFFSET>] | Default: **1**. Specifies the index of the first basepair on a chromosome, which would usually either be 0 or 1. This setting applies both to positions or regions data provided as input or generated as output. |
| `--regions-half-open` | \[yes \| **no**] | Default: **no**. Specifies whether the end position of a region interval is considered to be outside the region. If yes, the region is a “half open interval” in that it includes its start position but not its end position; if no (the default), the region is a “closed interval” in that it includes both its start and end positions. This setting applies to regions data provided as input or generated as output. |
| `--region-position-margin` | \[\<BASES>\[SUFFIX]] | Default: **0**. Defines an extra margin beyond the boundaries of all genomic regions within which a position will still be considered a match with the region. With no suffix or a “b” suffix the margin is interpreted as basepairs; with a “kb” or “mb” suffix it is measured in kilobases or megabases, respectively. |
| `--region-match-percent` | \[\<INT PERCENT>] | Default: **100**. Defines the minimum proportion of overlap between two regions in order to consider them a match. The percentage is measured in terms of the shorter region, such that 100% overlap always implies one region equal to or completely contained within the other. When combined with `--region-match-bases`, both requirements are enforced independently. For this reason, the default value for `--region-match-percent` is ignored if `--region-match-bases` is used alone. |
| `--region-match-bases` | \[\<BASES>\[SUFFIX]] | Default: **0**. Defines the minimum number of basepairs of overlap between two regions in order to consider them a match. With no suffix or a “b” suffix the overlap is interpreted as basepairs; with a “kb” or “mb” suffix it is measured in kilobases or megabases, respectively. When combined with `--region-match-percent`, both requirements are enforced independently. |


# Model Building Options

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--maximum-model-count` | `\[\<INT COUNT>]` | Default: **0**. Limits the number of models that will be generated, in order to reduce processing time. A value of 0 (the default) means no limit. |
| `--alternate-model-filtering` | [yes \| **no**] | Default: **no**. When enabled, the primary input dataset is only applied to one side of a generated model, while the alternate input dataset is applied to the other. When disabled (the default), the primary input dataset applies to both sides of each model. |
| `--all-pairwise-models` | [yes \| **no**] | Default: **no**. When enabled, model generation results in all possible pairwise combinations of data which conform to the primary and alternate input datasets. Note that this means the models have no score or ranking, since the prior knowledge is not searched for patterns. When disabled (the default), models are only generated which are supported by one or more groupings within the prior knowledge database. |
| `--maximum-model-group-size` | `\[\<INT SIZE>]` | Default: **30**. Limits the size of a grouping in the prior knowledge which can be used as part of a model generation analysis; any group which contains more genes than this limit is ignored for purposes of model generation. A value of 0 means no limit. |
| `--minimum-model-score` | `\[\<INT SCORE>]` | Default: **2**. Sets the minimum source-tally score for generated model; a model must be supported by groups from at least this many sources in order to be returned. |
| `--sort-models` | [**yes** \| no] | Default: **yes**. When enabled (the default), models are output in descending order by score. When combined with `--maximum-model-count`, this guarantees that only the highest-scoring models are output. When disabled, models are output in an unpredictable order. |


# Output Options

| **Option** | **Arguments** | **Information** |
|---|---|---|
| `--quiet` | [yes \| **no**] | Default: **no**. When enabled, no warnings or informational messages are printed to the screen. However, all information is still written to the log file, and certain unrecoverable errors are still printed to the screen. |
| `--verbose` | [yes \| **no**] | Default: **no**. When enabled, informational messages are printed to the screen in addition to warnings and errors. |
| `--prefix` | \[PREFIX] | Default: **biofilter**. Sets the prefix for all output filenames, which is then combined with a unique suffix for each type of output. The prefix may contain an absolute or relative path in order to write output to a different directory. |
| `--overwrite` | [yes \| **no**] | Default: **no**. Allows Biofilter to erase and overwrite any output file which already exists. When disabled (the default), Biofilter exits with an error to prevent any existing files from being overwritten. |
| `--stdout` | [yes \| **no**] | Default: **no**. Causes all output data to be written directly to the screen rather than saved to a file. On most platforms this output can then be sent directly into another program. |
| `--report-invalid-input` | [yes \| **no**] | Default: **no**. Causes any input data which was not understood by Biofilter to be copied into a separate output report file. This file also includes comments describing the error with each piece of data. |