#!/bin/bash -l

# Workflow specification

# Set up all configurable variables and filenames
search_regex='\.fastq\.?((gz)|(bz2)|(xz))?$'
exclude_regex='(trim)|(forward)|(reverse)'
data_fastq='data-fastq.txt'
external_data_fastq='external-data-fastq.txt'
combined_fastq='all-fastq-archive-list.txt'
ignore_files='pipeline/data-archiver/ignored-files.txt'
archive_mapping='pipeline/data-archiver/archived-mapping.txt'
filtered_fastq='filtered-fastq.txt'

# Activate conda environment
conda activate base

# Get all fastq file with find_files.py on 'data' and 'external_data' directories, excluding files with 
# 'trim', 'forward', or 'reverse' in their filenames.
python ~/pipeline/data-archiver/find_files.py data "$search_regex" -e "$exclude_regex" -o "$data_fastq"
python ~/pipeline/data-archiver/find_files.py external_data "$search_regex" -e "$exclude_regex" -o "$external_data_fastq"

# Combine the two lists of fastq files into one.
cat "$data_fastq" "$external_data_fastq" > "$combined_fastq"
rm "$data_fastq" "$external_data_fastq"

# Now take the previously generated list of *all* fastq files and filter out those that have already
# been archived (or should be ignored), using the archive mapping file.
python ~/pipeline/data-archiver/filter_new_files.py -i "$combined_fastq" -g "$ignore_files" -a "$archive_mapping" -o "$filtered_fastq"

# Get the list of files to archive and use archive_organizer.py to create a tab-delimited file mapping 
# files to archive locations.
python ~/pipeline/data-archiver/archive_organizer.py "$filtered_fastq" -o new-fastq-archive-mapping.txt