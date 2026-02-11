#!/bin/bash -l

# Set of steps to compare the list of files to archive with the list of files already in S3, 
# and generate a list of files that need to be archived, and a mapping file for those that 
# have already been archived.

# Get all s3 buckets - keep only lines that match a regex for directories (which start with "DIR") 
# and then extract the bucket names (which are the 2nd column).
s3cmd la | perl -pe 's/[\t\f ]+/ /gi' | perl -lne "print if m/^\s+DIR/" | cut -d' ' -f3 | \
    sort -k1,1 -u > all-s3-buckets.txt

# then fix up to only include non-duplicate main buckets
for i in `cat all-s3-buckets.txt`; do s3cmd ls --recursive $i | perl -pe 's/[\t\f ]+/ /gi' >> all-s3-data.txt; done
# Now extract only files using a perl regex (this is grabbing the 3rd column, which is the filesize column, and directories
# don't have a size so there is no number there).
cat all-s3-data.txt | perl -F$' ' -ne 'print if (split)[2] =~ /^[0-9]/' > s3-files.txt
rm all-s3-buckets.txt all-s3-data.txt

# Compare s3 files to the list of files to archive and generate a list of unmatched files and a mapping 
# file for the matched files. The unmatched files are those that need to be archived, and the mapping file 
# is for those that have already been archived.
cat new-fastq-archive-mapping.txt | cut -f1 | sort -k1,1 -u > local-files.txt
python ~/pipeline/data-archiver/compare_s3_files.py -l local-files.txt -s s3-files.txt \
    -u unmatched-files.txt -m s3-mappings.txt -w 14 -v
rm local-files.txt s3-files.txt

# Then convert s3-mappings.txt to 3 column format (local file, archive directory, archive filename) for use in the archive workflow.
printf "%s\t%s\t%s\n" "original_file_path" "archive_directory" "archived_file_path" > s3-mapping-formatted.txt
cat s3-mappings.txt | perl -pe 's/[\t\f ]+/ /gi' | perl -lne "print unless m/^#/" | awk '{print $1, "\t\t", $2}' >> s3-mapping-formatted.txt

# Then need/easiest to go and manually update the 'archive_directory' column with something like Excel