# Library of Knowledge Integration ('LOKI')
Rather than issuing queries in real-time to a series of external databases, Biofilter consults a local database called the Library of Knowledge Integration, or LOKI. This local repository contains all the knowledge from bulk downloads of the raw data from each external source. 

![loki schema](../images/2024-loki-biofilter-v3-schema.png)

LOKI must be generated on the local system before Biofilter can be used, but because the resulting knowledge database is a single local file, Biofilter itself does not require an internet connection to run. The process of building LOKI requires a relatively large amount of time and disk space to complete, but only needs to be done occasionally to incorporate updated data files from the various sources. 
