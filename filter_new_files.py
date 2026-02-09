#!/usr/bin/env python3
"""
Filter new files for archiving by removing already-archived and ignored files.

This script takes a list of newly identified files, cross-references them with
already archived files and optionally an ignore list, then outputs only the
new files that need to be archived.

The output can be passed directly to archive_organizer.py to create archive mappings.
"""

import argparse
import sys
from pathlib import Path
from typing import Set


def load_archived_files(mapping_file: Path) -> Set[str]:
    """
    Load the set of already archived file paths from a mapping file.
    
    Args:
        mapping_file: Path to the archive mapping file (TSV format)
        
    Returns:
        Set of file paths that are already archived
    """
    archived_files = set()
    
    if not mapping_file.exists():
        print(f"Warning: Archive mapping file not found: {mapping_file}", file=sys.stderr)
        return archived_files
    
    with open(mapping_file, 'r') as f:
        # Skip the header line
        header = f.readline()
        
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Split by tab - first column is the original file path
            parts = line.split('\t')
            if parts:
                archived_files.add(parts[0].strip())
    
    return archived_files


def load_ignore_list(ignore_file: Path) -> Set[str]:
    """
    Load the set of files to ignore from an ignore list file.
    
    Args:
        ignore_file: Path to the ignore list file (one path per line)
        
    Returns:
        Set of file paths to ignore
    """
    ignore_files = set()
    
    if not ignore_file.exists():
        print(f"Warning: Ignore list file not found: {ignore_file}", file=sys.stderr)
        return ignore_files
    
    with open(ignore_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Allow comments
                ignore_files.add(line)
    
    return ignore_files


def load_new_files(input_file: Path) -> list:
    """
    Load the list of new files to potentially archive.
    
    Args:
        input_file: Path to the file containing new file paths (one per line)
        
    Returns:
        List of file paths from the input file
    """
    new_files = []
    
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Allow comments
                new_files.append(line)
    
    return new_files


def filter_files(new_files: list, archived_files: Set[str], ignore_files: Set[str]) -> list:
    """
    Filter out already archived and ignored files from the new files list.
    
    Args:
        new_files: List of new file paths
        archived_files: Set of already archived file paths
        ignore_files: Set of file paths to ignore
        
    Returns:
        List of files that are truly new and should be archived
    """
    filtered_files = []
    
    for filepath in new_files:
        if filepath in archived_files:
            continue
        if filepath in ignore_files:
            continue
        filtered_files.append(filepath)
    
    return filtered_files


def write_output(output_file: Path, filtered_files: list):
    """
    Write the filtered file list to an output file.
    
    Args:
        output_file: Path to the output file
        filtered_files: List of filtered file paths
    """
    with open(output_file, 'w') as f:
        for filepath in filtered_files:
            f.write(filepath + '\n')


def main():
    parser = argparse.ArgumentParser(
        description='Filter new files for archiving by removing already-archived and ignored files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with archived files only
  %(prog)s -i new-files.txt -a archived-mapping.txt -o files-to-archive.txt
  
  # Include an ignore list
  %(prog)s -i new-files.txt -a archived-mapping.txt -g ignore-list.txt -o files-to-archive.txt
  
  # Use with archive_organizer.py
  %(prog)s -i new-files.txt -a archived-mapping.txt -o filtered.txt
  python archive_organizer.py -i filtered.txt -o new-archive-mapping.txt
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        type=Path,
        required=True,
        help='Input file containing list of new files to potentially archive (one path per line)'
    )
    
    parser.add_argument(
        '-a', '--archived',
        type=Path,
        required=True,
        help='Archive mapping file (TSV format with header, first column is original file path)'
    )
    
    parser.add_argument(
        '-g', '--ignore',
        type=Path,
        help='Optional ignore list file (one path per line, # for comments)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output file for filtered list of files to archive'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Print verbose statistics about filtering'
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # Load the archived files
    if args.verbose:
        print(f"Loading archived files from: {args.archived}")
    archived_files = load_archived_files(args.archived)
    if args.verbose:
        print(f"  Found {len(archived_files)} archived files")
    
    # Load the ignore list if provided
    ignore_files = set()
    if args.ignore:
        if args.verbose:
            print(f"Loading ignore list from: {args.ignore}")
        ignore_files = load_ignore_list(args.ignore)
        if args.verbose:
            print(f"  Found {len(ignore_files)} files to ignore")
    
    # Load new files
    if args.verbose:
        print(f"Loading new files from: {args.input}")
    new_files = load_new_files(args.input)
    if args.verbose:
        print(f"  Found {len(new_files)} new files")
    
    # Filter the files
    if args.verbose:
        print("Filtering files...")
    filtered_files = filter_files(new_files, archived_files, ignore_files)
    
    # Write output
    write_output(args.output, filtered_files)
    
    # Print summary
    num_filtered_out = len(new_files) - len(filtered_files)
    print(f"\nSummary:")
    print(f"  Input files:           {len(new_files)}")
    print(f"  Already archived:      {len([f for f in new_files if f in archived_files])}")
    print(f"  Ignored:               {len([f for f in new_files if f in ignore_files])}")
    print(f"  New files to archive:  {len(filtered_files)}")
    print(f"\nFiltered file list written to: {args.output}")
    
    if len(filtered_files) == 0:
        print("\n⚠ No new files to archive!")
    else:
        print(f"\n✓ Ready to pass to archive_organizer.py")


if __name__ == '__main__':
    main()
