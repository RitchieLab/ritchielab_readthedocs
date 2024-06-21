# PARIS

Biofilter now incorporates the PARIS algorithm, which stands for Pathway Analysis by Randomization Incorporating Structure. This analysis uses a randomized permutation test to indicate whether significant SNP associations are statistically over-represented in a pathway or other gene grouping, accounting for population-specific genetic structure. Further details about the design and behavior of the PARIS algorithm can be found in these publications:
* Yaspan, et al. 2011. Genetic analysis of biological pathway data through genomic
randomization. Hum Genet. May;129(5):563-71.
* Butkiewicz and Cooke Bailey, et al. 2016. Pathway analysis by randomization incorporating
structure—PARIS: an update. Bioinformatics. In Press.

# PARIS Inputs

The first kind of input data that PARIS requires is SNPs or positions, including their p-values from a
prior association test. These can be provided using Biofilter's regular SNP or POSITION options (or
their _FILE counterparts), along with two "extra" columns specifically for PARIS: the chromosome of
the marker, and its p-value from the association test results.

For SNPs, the input format might therefore look like this:
```
#rs     chr     pval
rs123   1       0.1234
rs456   7       1.3e-5
rs789   0       0.5678
```

For position input, the extra columns can only be recognized when the position data is provided in fullwidth format, which is a 4-column .map file format. The resulting input for PARIS might look like this:
```
#chr    label   ignored     position        chr     pval
1       rs123   -           123456789       1       0.1234
7       rs456   -           456456456       7       1.3e-5
9       rs789   -           987654321       9       0.5678
```

Note that this does imply a duplication of the "`chr`" column for positional input to PARIS; this is a result of PARIS' unified "extra" columns processing and backwards compatibility with PARIS v1.0 inputs. The purpose of the extra chromosome column for SNP input was to allow for specificity when dealing with SNPs that are mapped to more than one chromosome, by specifying which instance should be considered for the analysis. This feature can be disabled with the PARIS_ENFORCE_INPUT_CHROMOSOME option, in which case the second chromosome column of the positional input format is ignored and may be filled with any placeholder, such as "-".

The other kind of input data that PARIS requires are the LD blocks of the population being studied. These are input using the regular `REGION_FILE` options, with no extra columns required. A few precalled LD block files, based on HapMap population data, are available from the [Biofilter software download page](https://ritchielab.org/software/biofilter-download-1), but you may also prefer to generate your own LD blocks based on the population of your dataset.

# PARIS Outputs

The PARIS module is invoked using the PARIS option. This will produce an output file `PREFIX.paris-summary` which lists information about each analyzed pathway, its feature profile in terms of the PARIS algorithm, and PARIS' permutation-derived p-value for the significance of the results in that pathway. For example:
```
group           desc    genes   features    simple      (sig)   complex     (sig)       pval
path:hsa00010           61      168         110         2       58          9           0.877
path:hsa00020           30      58          30          1       28          2           0.894
…
```

The first three columns of this output specify the pathway or other gene group that has been analyzed, including its label, longer description (if any) and the number of genes it contains. The next five columns reflect the group's "fingerprint" for the purposes of PARIS analysis, including the total number of genomic features that it covers, the breakdown of how many of those features were simple (containing only one input SNP) or complex (an LD block containing multiple input SNPs), and the portion of those subtotals which were significant according to their association p-values provided along with the input SNPs or positions. The final column is the PARIS p-value, which reflects the probability of a randomized gene grouping with the same feature fingerprint containing at least as many significant features as the original.

In addition to this summary output, the PARIS_DETAILS option will produce an output file `PREFIX.paris-detail` which is broken into separate sections for each analyzed pathway or other gene grouping. Within each section, the PARIS algorithm is applied separately to the individual genes in that pathway, with a similar output format:
```
Pathway Investigation: path:hsa00010
Glycolysis / Gluconeogenesis
genes   features    simple      (sig)   complex     (sig)       pval
61      168         110         2       58          9           0.877
Gene Breakdown: path:hsa00010
gene        features    simple      (sig)       complex     (sig)       pval
PCK1        3           3           0           0           0           1
PFKP        29          23          1           6           0           0.573
LDHA        1           0           0           1           1           < 0.001
ALDH9A1     5           3           0           2           1           0.108
```
This can help to identify, for example, if a strong pathway-level signal is being driven by only one or a few genes, or if strong signals in several pathways are all driven by the same shared gene.

# PARIS Example

Putting it all together, a simple PARIS run might look like this:

`biofilter.py --knowledge loki.db –-snp-file my_CEU_study.snps –-region-file ld_CEU_b38.regions –-paris –-paris-details –-prefix my_CEU_study`

In this example, two output files would be generated: `my_CEU_study.paris-summary` and `my_CEU_study.paris-detail`.

# Additional PARIS Options

This section describes the standard configuration options that have a special significance when running a PARIS analysis, as well as the PARIS-specific options that have no meaning for Biofilter's other modes (such as filtering, annotation and model building).

| **Option** | **Argument** | **Information** |
|---|---|---|
|`--region-position-margin / REGION_POSITION_MARGIN`|\<bases>|Default: **0**. This option influences the step of the PARIS algorithm which matches input SNPs or positions to LD block regions. With a positive value, this option will cause PAIRS to consider a SNP or position to be part of a complex LD feature even if it is some distance outside the boundaries of that feature region.|
|`--region-match-percent / REGION_MATCH_PERCENT`|\<percentage>|Default: **100**. See below.|
|`--region-match-bases / REGION_MATCH_BASES`|\<bases>|Default: **0**. These two options influence the step of the PARIS algorithm which matches features (both simple and complex) to gene regions. The default behavior of the original PARIS algorithm was to consider a feature to be covered by a gene (and thus its pathways) if the feature and the gene region shared at least one base pair; this behavior can be replicated by setting `REGION_MATCH_BASES` to 1. A higher value will require more overlap between the feature and the gene region to be considered a match, and a negative value can be used to allow some margin between a feature region and a gene (i.e. two regions can "match" with "-100 bases" if the end of one is within 100 basepairs of the beginning of the other). Alternatively, `REGION_MATCH_PERCENT` can specify the portion of the smaller region that must be covered by the larger region.|
|`--paris-p-value / PARIS_P_VALUE`|\<p-value>|Default: **0.05**. This option controls the p-value threshold of the association testing results input that PARIS will consider to be significant.|
|`--paris-zero-p-values / PARIS_ZERO_P_VALUES`|[**ignore**/ insignificant/ significant]|Default: **ignore**. This option tells PARIS what to do with inputs that report a p-value of zero. The default behavior is to 'ignore' them, as if the SNP or position was not part of the input at all. Alternatively, PARIS can treat them as if they are 'significant' (which may be appropriate if the prior testing can produce p-values below the machine precision threshold, which would then "round down" to zero), or PARIS can consider them 'insigifnicant' (which may be more appropriate if the prior test can encounter an error condition which manifests as a p-value of zero).|
|`--paris-max-p-value / PARIS_MAX_P_VALUE`|\<p-value>|Default: none. This option serves as a run-time speed optimization by specifying the highest permutation derived pvalue that PARIS could yield which would have any meaning. Because of the way PARIS' permutation testing is implemented, the analysis can be cut short as soon as it is determined that a gene or pathway is going to end up higher than this threshold. For example, with a setting of 0.5, PARIS can stop early and report a p-value of ">= 0.5" without spending the extra time to determine if it would have been 0.51, or 0.75, or 0.99, etc.|
|`--paris-enforce-input-chromosome / PARIS_ENFORCE_INPUT_CHROMOSOME`|[**yes**/no]|Default: **yes**. In addition to their association test p-value results, PARIS expects input SNPs or positions to have a second extra column which specifies their chromosome. In keeping with the behavior of PARIS v1, this extra chromosome column will be used to filter out any SNPs that are found on a different chromosome in the LOKI knowledge database. By disabling this option, the extra chromosome column will be ignored (and can be left blank) and PARIS will process SNPs on whatever chromosome it believes they are found on.|
|`--paris-permutation-count / PARIS_PERMUTATION_COUNT`|\<count>|Default: **1000**. By default, PARIS will perform 1000 permutations of each analyzed gene or pathway, which yields a pvalue precision of 1 / 1000 or 0.001. The number of permutations can be changed using this option to yield a different precision, at the cost of higher running time for more permutations.|
|`--paris-bin-size / PARIS_BIN_SIZE`|\<size>|Default: **10000**. When PARIS permutes a gene or pathway, it must randomly replace all of its features with other features of similar size (which is measured in terms of the number of input SNPs or positions within the feature, not the total chromosomal region size in basepairs). To determine the eligible candidates for this replacement, PARIS will sort all features by their size and then divide them into some number of "bins" such that each bin contains approximately 10000 features. This option can be used to change the target number of features per bin, which will in turn change the number of candidate replacements for each feature during the permutation step.|
|`--paris-snp-file / PARIS_SNP_FILE`|\<file> [file] […]|Default: none. See below.|
|`--paris-position-file / PARIS_POSITION_FILE`|\<file> [file] […]|Default: none. These two options serve as run-time memory usage optimizations by allowing PARIS to read and summarize the input SNP or position data on the fly, instead of having to load it completely into memory using Biofilter's standard input options (`SNP`, `SNP_FILE`, `POSITION` or `POSITION_FILE`).|
|`--paris-details / PARIS_DETAILS`|[yes/**no**]|Default: **no**. By default, PARIS will only generate "summary" output of the permutation results of each analyzed pathway. With this option enabled, PARIS will also generate a "details" output which further breaks down the analysis into each individual gene of each analyzed pathway.|