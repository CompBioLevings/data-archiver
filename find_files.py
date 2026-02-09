#!/usr/bin/env python3
"""
Recursively find files with specific extensions, with optional regex-based exclusion.
"""

import argparse
import os
import re
from pathlib import Path
from typing import List, Optional, Pattern, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing


def should_exclude(filepath: str, 
                   filename: str,
                   exclude_pattern: Optional[Pattern],
                   exclude_in_path: bool) -> bool:
    """
    Check if file should be excluded based on exclusion pattern.
    
    Args:
        filepath: Full file path
        filename: Just the filename
        exclude_pattern: Compiled exclusion regex pattern
        exclude_in_path: Whether to match in full path (True) or just filename (False)
        
    Returns:
        True if file should be excluded, False otherwise
    """
    if exclude_pattern is None:
        return False
    
    if exclude_in_path:
        return exclude_pattern.search(filepath) is not None
    else:
        return exclude_pattern.search(filename) is not None


def scan_directory_chunk(args: tuple) -> List[str]:
    """
    Scan a directory for files matching criteria.
    
    Args:
        args: Tuple of (directory, extension_pattern, exclude_pattern, exclude_in_path)
        
    Returns:
        List of matching file paths
    """
    directory, extension_pattern, exclude_pattern, exclude_in_path = args
    matching_files = []
    
    try:
        for entry in os.scandir(directory):
            try:
                if entry.is_file(follow_symlinks=False):
                    filepath = entry.path
                    filename = entry.name
                    
                    # Check if extension matches
                    if extension_pattern.search(filename):
                        # Check if should be excluded
                        if not should_exclude(filepath, filename, exclude_pattern, exclude_in_path):
                            matching_files.append(filepath)
                            
            except (PermissionError, OSError):
                # Skip files we can't access
                continue
                
    except (PermissionError, OSError):
        # Skip directories we can't access
        pass
    
    return matching_files


def scan_subdirectories(directory: str) -> List[str]:
    """
    Get immediate subdirectories of a directory.
    
    Args:
        directory: Directory to scan
        
    Returns:
        List of subdirectory paths
    """
    subdirs = []
    try:
        for entry in os.scandir(directory):
            try:
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(entry.path)
            except (PermissionError, OSError):
                continue
    except (PermissionError, OSError):
        pass
    
    return subdirs


def get_all_directories_parallel(root_path: str, num_workers: Optional[int] = None) -> List[str]:
    """
    Get all subdirectories recursively using parallel scanning.
    
    Args:
        root_path: Root directory to start from
        num_workers: Number of parallel workers (default: CPU count - 2)
        
    Returns:
        List of all directory paths
    """
    if num_workers is None:
        num_workers = max([multiprocessing.cpu_count() - 2, 1])
    
    all_directories = [root_path]
    directories_to_scan = [root_path]
    seen_directories: Set[str] = {root_path}
    
    # Use ThreadPoolExecutor for I/O-bound directory scanning
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        while directories_to_scan:
            # Submit all current directories for scanning
            futures = {executor.submit(scan_subdirectories, d): d 
                      for d in directories_to_scan}
            
            # Clear the list for the next batch
            directories_to_scan = []
            
            # Collect results
            for future in as_completed(futures):
                try:
                    subdirs = future.result()
                    for subdir in subdirs:
                        # Avoid duplicates
                        if subdir not in seen_directories:
                            seen_directories.add(subdir)
                            all_directories.append(subdir)
                            directories_to_scan.append(subdir)
                except Exception:
                    # Continue even if one directory fails
                    pass
    
    return all_directories


def find_files(root_path: str,
               extension_pattern: str,
               case_insensitive: bool = True,
               exclude_pattern: Optional[str] = None,
               exclude_in_path: bool = False,
               num_workers: Optional[int] = None) -> List[str]:
    r"""
    Find all files matching extension pattern, optionally excluding based on pattern.
    
    Args:
        root_path: Root directory to search
        extension_pattern: Perl regex pattern for file matching (e.g., '\.fastq\.?((gz)|(bz2)|(xz))?')
        case_insensitive: Whether matching should be case-insensitive
        exclude_pattern: Optional Perl regex pattern for exclusion (e.g., '(trim)|(forward)|(reverse)')
        exclude_in_path: Whether to match exclusion in full path (True) or just filename (False)
        num_workers: Number of parallel workers (default: CPU count - 2)
        
    Returns:
        List of matching file paths
    """
    # Compile extension pattern
    if case_insensitive:
        extension_regex = re.compile(extension_pattern, re.IGNORECASE)
    else:
        extension_regex = re.compile(extension_pattern)
    
    # Compile exclusion pattern if provided
    exclude_regex: Optional[Pattern] = None
    if exclude_pattern:
        if case_insensitive:
            exclude_regex = re.compile(exclude_pattern, re.IGNORECASE)
        else:
            exclude_regex = re.compile(exclude_pattern)
    
    # Determine number of workers
    if num_workers is None:
        num_workers = max([multiprocessing.cpu_count() - 2, 1])
    
    # Get all directories using parallel scanning
    print(f"Discovering directory tree...", flush=True)
    all_directories = get_all_directories_parallel(root_path, num_workers)
    print(f"Found {len(all_directories)} directories to scan", flush=True)
    
    # Prepare arguments for each directory
    dir_args = [(d, extension_regex, exclude_regex, exclude_in_path) 
                for d in all_directories]
    
    # Process directories in parallel
    matching_files: List[str] = []
    
    print(f"Scanning directories for matching files...", flush=True)
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(scan_directory_chunk, arg) for arg in dir_args]
        
        for future in as_completed(futures):
            try:
                matching_files.extend(future.result())
            except Exception as e:
                # Continue even if one chunk fails
                pass
    
    return matching_files


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Recursively find files matching Perl regex pattern with optional exclusion.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
Examples:
  # Find all .fastq files (and compressed variants)
  %(prog)s /data/project '\.fastq\.?((gz)|(bz2)|(xz))?$'
  
  # Find all .fastq files, exclude any with "trim", "forward", or "reverse" in filename
  %(prog)s /data/project '\.fastq\.?((gz)|(bz2)|(xz))?$' -e '(trim)|(forward)|(reverse)'
  
  # Find all .fastq files, exclude any with "trim" anywhere in path
  %(prog)s /data/project '\.fastq\.?((gz)|(bz2)|(xz))?$' -e '.*trim.*' --exclude-in-path
  
  # Find all .bam files excluding temp files
  %(prog)s /data/project '\.bam$' -e '(temp)|(tmp)'
  
  # Use 8 parallel workers
  %(prog)s /data/project '\.fastq$' -w 8
  
  # Case-sensitive matching
  %(prog)s /data/project '\.FASTQ$' --case-sensitive
        """
    )
    parser.add_argument(
        'directory',
        help='Root directory to search recursively'
    )
    parser.add_argument(
        'pattern',
        help=r"Perl regex pattern for file matching (e.g., '\.fastq\.?((gz)|(bz2)|(xz))?$')"
    )
    parser.add_argument(
        '-e', '--exclude',
        metavar='PATTERN',
        help=r"Perl regex pattern to exclude files (e.g., '(trim)|(forward)|(reverse)')"
    )
    parser.add_argument(
        '--exclude-in-path',
        action='store_true',
        help='Match exclusion pattern in full filepath instead of just filename'
    )
    parser.add_argument(
        '--case-sensitive',
        action='store_true',
        help='Make pattern matching case-sensitive (default: case-insensitive)'
    )
    parser.add_argument(
        '-o', '--output',
        metavar='FILE',
        help='Output file for results (default: print to stdout)'
    )
    parser.add_argument(
        '-w', '--workers',
        type=int,
        metavar='N',
        help='Number of parallel workers (default: CPU count - 2)'
    )
    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing (use single thread)'
    )
    parser.add_argument(
        '--relative',
        action='store_true',
        help='Output relative paths instead of absolute paths'
    )
    
    args = parser.parse_args()
    
    # Validate directory
    if not os.path.isdir(args.directory):
        parser.error(f"Directory does not exist: {args.directory}")
    
    # Convert to absolute path
    root_path = os.path.abspath(args.directory)
    
    # Determine number of workers
    num_workers = 1 if args.no_parallel else args.workers
    
    # Determine case sensitivity
    case_insensitive = not args.case_sensitive
    
    # Find files
    print(f"Searching for files matching pattern '{args.pattern}' in: {root_path}", flush=True)
    if args.exclude:
        location = "filepath" if args.exclude_in_path else "filename"
        print(f"Excluding files matching pattern '{args.exclude}' in {location}", flush=True)
    
    case_mode = "case-insensitive" if case_insensitive else "case-sensitive"
    print(f"Matching mode: {case_mode}", flush=True)
    
    worker_count = num_workers if num_workers else max([multiprocessing.cpu_count() - 2, 1])
    print(f"Using {worker_count} worker(s)", flush=True)
    print("", flush=True)
    
    matching_files = find_files(
        root_path=root_path,
        extension_pattern=args.pattern,
        case_insensitive=case_insensitive,
        exclude_pattern=args.exclude,
        exclude_in_path=args.exclude_in_path,
        num_workers=num_workers
    )
    
    # Sort results
    matching_files.sort()
    
    # Convert to relative paths if requested
    if args.relative:
        matching_files = [os.path.relpath(f, root_path) for f in matching_files]
    
    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            for filepath in matching_files:
                f.write(filepath + '\n')
        print(f"Found {len(matching_files)} matching files", flush=True)
        print(f"Results written to: {args.output}", flush=True)
    else:
        for filepath in matching_files:
            print(filepath)
        print(f"\nFound {len(matching_files)} matching files", flush=True)


if __name__ == '__main__':
    main()