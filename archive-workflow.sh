#!/bin/bash -l

# Workflow specification

# Set up all configurable variables and filenames
search_regex='\.fastq\.?((gz)|(bz2)|(xz))?$'
exclude_regex='(trim)|(forward)|(reverse)'
data_fastq='data-fastq.txt'
external_data_fastq='external-data-fastq.txt'
combined_fastq='all-fastq-archive-list.txt'
ignore_files='pipeline/data-archiver/data/ignored-files.txt'
archive_mapping='pipeline/data-archiver/data/archived-mapping.txt'
filtered_fastq='filtered-fastq.txt'
archive_additions='new-fastq-archive-mapping.txt'
num_workers=4

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

# Exit script here if there are no new files to archive.
if [ ! -s "$filtered_fastq" ]; then
    echo "No new files to archive. Exiting."
    rm "$combined_fastq" "$filtered_fastq"
    exit 0
fi

# Get the list of files to archive and use archive_organizer.py to create a tab-delimited file mapping 
# files to archive locations.
python ~/pipeline/data-archiver/archive_organizer.py "$filtered_fastq" -o "$archive_additions" --ignore-file "$ignore_files"
rm "$combined_fastq" "$filtered_fastq"

# Now sync to the Tier 2 storage (using s3cmd) based on the generated mapping file.
python ~/pipeline/data-archiver/s3_sync.py "$archive_additions" --workers "$num_workers"

# Finally add the new archive additions to the 'master' archive mapping file.  Make sure to remove any lines that 
# start with "original_file_path" (i.e. the header line) from the additions file before appending to the master mapping file.
grep -v '^original_file_path' "$archive_additions" >> "$archive_mapping"
rm "$archive_additions"

# Now get the date and time of the last successful archive sync, then create a GZIP-compressed file of the 
# archive mapping file and ignore file with a timestamp in the archive name for record-keeping. Use 7z to #
# encrypt and add password to the archive mapping file and ignored file - take the password as user input
timestamp=$(date +"%Y%m%d_%H%M%S")
read -s -p "Enter password to encrypt archive mapping and ignored files: " password && echo ""
7za a -p"$password" "pipeline/data-archiver/data/archiver-db-bkup_${timestamp}.7z" "$archive_mapping" "$ignore_files"
unset password

# To 'reset' in case of error, change dir to pipeline and extract database zipped archive and overwrite old files
# cd ~/pipeline/data-archiver/data/
# 7za x "archiver-db-bkup_*.7z"