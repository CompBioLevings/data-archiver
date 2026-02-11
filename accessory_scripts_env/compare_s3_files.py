#!/usr/bin/env python3
"""
Compare local file list with AWS S3 bucket contents and identify matches/non-matches.

This script compares filenames (not full paths) between a local file list and
S3 bucket contents, producing two outputs:
1. Unmatched files - local files that don't have a matching filename in S3
2. Matched mappings - local files matched with their S3 location(s)
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Set, Tuple


def parse_s3_listing(s3_file: Path) -> Dict[str, List[str]]:
    """
    Parse AWS S3 bucket listing file.
    
    Expected format from 'aws s3 ls --recursive':
    2019-08-16 18:22:45   1234  s3://bucket/path/to/file.ext
    or
    2019-08-16 18:22:45  DIROBJ  s3://bucket/path/
    
    Args:
        s3_file: Path to file containing S3 listing
        
    Returns:
        Dictionary mapping filename to list of full S3 paths
    """
    filename_to_s3paths: Dict[str, List[str]] = defaultdict(list)
    
    # Pattern to match S3 listing format
    # Date Time Size s3://path or Date Time DIROBJ s3://path
    pattern = re.compile(r'^\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(?:\d+[KMGT]?|DIROBJ)\s+(s3://.+)$')
    
    with open(s3_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            match = pattern.match(line)
            if match:
                s3_path = match.group(1)
                
                # Skip directories (paths ending with /)
                if s3_path.endswith('/'):
                    continue
                
                # Extract filename from S3 path
                filename = os.path.basename(s3_path)
                
                # Store mapping
                filename_to_s3paths[filename].append(s3_path)
    
    return filename_to_s3paths


def parse_local_file_list(local_file: Path) -> List[str]:
    """
    Parse local file list (one path per line).
    
    Args:
        local_file: Path to file containing local file paths
        
    Returns:
        List of local file paths
    """
    local_paths = []
    
    with open(local_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                local_paths.append(line)
    
    return local_paths


def compare_file_chunk(local_paths_chunk: List[str], 
                      s3_filename_map: Dict[str, List[str]]) -> Tuple[List[str], List[Tuple[str, List[str]]]]:
    """
    Compare a chunk of local files with S3 files by filename.
    
    Args:
        local_paths_chunk: Chunk of local file paths to process
        s3_filename_map: Dictionary mapping filename to S3 paths
        
    Returns:
        Tuple of (unmatched_files, matched_mappings) for this chunk
    """
    unmatched_files = []
    matched_mappings = []
    
    for local_path in local_paths_chunk:
        filename = os.path.basename(local_path)
        
        if filename in s3_filename_map:
            # Found match(es) in S3
            s3_paths = s3_filename_map[filename]
            matched_mappings.append((local_path, s3_paths))
        else:
            # No match found
            unmatched_files.append(local_path)
    
    return unmatched_files, matched_mappings


def compare_files(local_paths: List[str], 
                 s3_filename_map: Dict[str, List[str]],
                 max_workers: int = None,
                 verbose: bool = False) -> Tuple[List[str], List[Tuple[str, List[str]]]]:
    """
    Compare local files with S3 files by filename using parallel processing.
    
    Args:
        local_paths: List of local file paths
        s3_filename_map: Dictionary mapping filename to S3 paths
        max_workers: Maximum number of worker processes (None = CPU count)
        verbose: Print progress information
        
    Returns:
        Tuple of (unmatched_files, matched_mappings)
        - unmatched_files: List of local paths with no S3 match
        - matched_mappings: List of (local_path, [s3_paths]) tuples
    """
    # If file list is small, don't bother with parallel processing
    if len(local_paths) < 100:
        return compare_file_chunk(local_paths, s3_filename_map)
    
    # Split local_paths into chunks for parallel processing
    if max_workers is None:
        max_workers = os.cpu_count() or 4
    
    chunk_size = max(1, len(local_paths) // (max_workers * 4))  # 4 chunks per worker
    chunks = [local_paths[i:i + chunk_size] for i in range(0, len(local_paths), chunk_size)]
    
    if verbose:
        print(f"  Using {max_workers} workers to process {len(chunks)} chunks")
        print(f"  Chunk size: ~{chunk_size} files")
    
    unmatched_files = []
    matched_mappings = []
    
    # Process chunks in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunks for processing
        future_to_chunk = {executor.submit(compare_file_chunk, chunk, s3_filename_map): i 
                          for i, chunk in enumerate(chunks)}
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_chunk):
            chunk_unmatched, chunk_matched = future.result()
            unmatched_files.extend(chunk_unmatched)
            matched_mappings.extend(chunk_matched)
            
            completed += 1
            if verbose and completed % max(1, len(chunks) // 10) == 0:
                progress = (completed / len(chunks)) * 100
                print(f"  Progress: {completed}/{len(chunks)} chunks ({progress:.1f}%)")
    
    return unmatched_files, matched_mappings


def write_unmatched_files(unmatched: List[str], output_file: Path):
    """
    Write unmatched files to output file.
    
    Args:
        unmatched: List of unmatched local file paths
        output_file: Path to output file
    """
    with open(output_file, 'w') as f:
        f.write("# Local files with no matching filename in S3\n")
        f.write("# Format: local_file_path\n")
        f.write("#" + "=" * 78 + "\n")
        for filepath in sorted(unmatched):
            f.write(f"{filepath}\n")
    
    print(f"Unmatched files written to: {output_file}")


def write_matched_mappings(matched: List[Tuple[str, List[str]]], output_file: Path):
    """
    Write matched file mappings to output file.
    
    Args:
        matched: List of (local_path, [s3_paths]) tuples
        output_file: Path to output file
    """
    with open(output_file, 'w') as f:
        f.write("# Mappings between local files and S3 locations\n")
        f.write("# Format: local_file_path<TAB>s3_path(s)\n")
        f.write("# Note: Multiple S3 paths are separated by ' | '\n")
        f.write("#" + "=" * 78 + "\n")
        
        for local_path, s3_paths in sorted(matched):
            # Join multiple S3 paths with separator
            s3_paths_str = ' | '.join(s3_paths)
            f.write(f"{local_path}\t{s3_paths_str}\n")
    
    print(f"Matched mappings written to: {output_file}")


def print_summary(local_count: int, 
                 unmatched_count: int, 
                 matched_count: int,
                 multi_match_count: int):
    """
    Print summary statistics.
    
    Args:
        local_count: Total number of local files
        unmatched_count: Number of unmatched files
        matched_count: Number of matched files
        multi_match_count: Number of files with multiple S3 matches
    """
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total local files:              {local_count}")
    print(f"Matched in S3:                  {matched_count} ({matched_count/local_count*100:.1f}%)")
    print(f"  - Multiple S3 matches:        {multi_match_count}")
    print(f"Not found in S3:                {unmatched_count} ({unmatched_count/local_count*100:.1f}%)")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Compare local file list with AWS S3 bucket contents.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  %(prog)s -l local-files.txt -s s3-listing.txt -u unmatched.txt -m mappings.txt
  
  # With verbose output
  %(prog)s -l local-files.txt -s s3-listing.txt -u unmatched.txt -m mappings.txt -v
  
  # Generate S3 listing and compare
  aws s3 ls --recursive s3://my-bucket/ > s3-contents.txt
  %(prog)s -l local-files.txt -s s3-contents.txt -u unmatched.txt -m mappings.txt

Note:
  - Matching is based on filename only, not full path
  - S3 listing file should be output from: aws s3 ls --recursive
  - If a filename appears multiple times in S3, all matches are recorded
        """
    )
    
    parser.add_argument(
        '-l', '--local-files',
        type=Path,
        required=True,
        help='Input file containing local file paths (one per line)'
    )
    
    parser.add_argument(
        '-s', '--s3-listing',
        type=Path,
        required=True,
        help='Input file containing S3 bucket listing (from aws s3 ls --recursive)'
    )
    
    parser.add_argument(
        '-u', '--unmatched-output',
        type=Path,
        required=True,
        help='Output file for unmatched local files'
    )
    
    parser.add_argument(
        '-m', '--matched-output',
        type=Path,
        required=True,
        help='Output file for matched file mappings (TSV format)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Print verbose information during processing'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of parallel workers (default: CPU count)'
    )
    
    args = parser.parse_args()
    
    # Validate input files exist
    if not args.local_files.exists():
        print(f"Error: Local file list not found: {args.local_files}", file=sys.stderr)
        sys.exit(1)
    
    if not args.s3_listing.exists():
        print(f"Error: S3 listing file not found: {args.s3_listing}", file=sys.stderr)
        sys.exit(1)
    
    # Parse S3 listing
    if args.verbose:
        print(f"Parsing S3 listing from: {args.s3_listing}")
    s3_filename_map = parse_s3_listing(args.s3_listing)
    if args.verbose:
        print(f"  Found {len(s3_filename_map)} unique filenames in S3")
        total_s3_files = sum(len(paths) for paths in s3_filename_map.values())
        print(f"  Total S3 file entries: {total_s3_files}")
    
    # Parse local file list
    if args.verbose:
        print(f"Parsing local file list from: {args.local_files}")
    local_paths = parse_local_file_list(args.local_files)
    if args.verbose:
        print(f"  Found {len(local_paths)} local files")
    
    # Compare files
    if args.verbose:
        print("Comparing files by filename...")
    unmatched_files, matched_mappings = compare_files(local_paths, s3_filename_map, 
                                                      args.workers, args.verbose)
    
    # Count files with multiple S3 matches
    multi_match_count = sum(1 for _, s3_paths in matched_mappings if len(s3_paths) > 1)
    
    # Write outputs
    write_unmatched_files(unmatched_files, args.unmatched_output)
    write_matched_mappings(matched_mappings, args.matched_output)
    
    # Print summary
    print_summary(len(local_paths), len(unmatched_files), len(matched_mappings), multi_match_count)
    
    if multi_match_count > 0:
        print(f"\n⚠ Warning: {multi_match_count} file(s) have multiple S3 locations")
        print("  Check the mappings file for details")


if __name__ == '__main__':
    main()
