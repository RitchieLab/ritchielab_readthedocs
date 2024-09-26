# Biofilter 3.0
<!-- TODO Add easy to read description of Biofilter -->
Biofilter is a tool that helps combine genetic and biological data to prioritize and analyze important genetic variations in complex traits. It takes data from different sources, like biological pathways and gene interactions, and combines them in one place. By organizing this data, Biofilter simplifies the process of identifying genetic variants involved in complex traits by offering a structured framework for managing and analyzing genetic information, saving users the time and effort of manually searching through individual databases.


# Library of Knowledge Integration ('LOKI')
Rather than issuing queries in real-time to a series of external databases, Biofilter consults a local database called the **Library of Knowledge Integration**, or nicknamed **LOKI**. 

This LOKI section will guide a user through how to run the provided scripts, dynamically pull raw data from each external source, and build a LOKI SQLite database.

![loki schema](../images/2024-loki-biofilter-v3-schema.png)

<!-- TODO Update with final LOKI figure -->


LOKI must be generated on the local system before Biofilter can be used, but because the resulting knowledge database is a single local file, Biofilter itself does not require an internet connection to run. 

The process of building LOKI requires a relatively large amount of time and disk space to complete, but only needs to be done once.
