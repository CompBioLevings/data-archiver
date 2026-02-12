# Lab Data Archiver  

This is a pipeline I put together to allow for semi-automated archiving of lab data from the Slattery Lab (to make sure all raw data are properly archived).  This first version is simply for archiving raw FASTQ sequencing files for our projects, but I may build out additional capabilities in the future.  To execute the pipeline/archive sequencing data, simply run the steps found in `archive-workflow.sh`, assigning the configurable variables in the beginning of the script appropriately.  

The pipeline uses `s3cmd` (and associated credentials) for syncing data to an s3 storage bucket for the *Tier 2* storage at the Minnesota Supercomputing Institute.  But, it also builds up a file with a listing of the paths to all archived files both on MSI primary/lab storage and in the Tier 2 s3 bucket.  This file could be useful for other copy/archive processes.

