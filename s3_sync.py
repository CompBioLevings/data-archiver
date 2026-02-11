#!/usr/bin/env python3
"""
Sync files to S3 based on archive mapping file using s3cmd.

This script reads the output from archive_organizer.py (a TSV file with columns:
original_file_path, archive_directory, archived_file_path) and uploads each file
to its designated S3 location using s3cmd.
"""

import argparse
import sys
import os
import subprocess
import shutil
from pathlib import Path
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


class FileMapping:
    """Represents a file to upload and its S3 destination."""
    def __init__(self, local_path: str, s3_path: str, archive_dir: str):
        self.local_path = local_path
        self.s3_path = s3_path
        self.archive_dir = archive_dir


def check_s3cmd_installed() -> Tuple[bool, Optional[str]]:
    """
    Check if s3cmd is installed and available.
    
    Returns:
        Tuple of (is_available, path_to_s3cmd)
    """
    s3cmd_path = shutil.which('s3cmd')
    if s3cmd_path:
        return True, s3cmd_path
    return False, None


def load_mapping_file(mapping_file: Path) -> List[FileMapping]:
    """
    Load file mappings from the archive mapping TSV file.
    
    Args:
        mapping_file: Path to the archive mapping file
        
    Returns:
        List of FileMapping objects
    """
    mappings = []
    
    with open(mapping_file, 'r') as f:
        # Read and validate header
        header = f.readline().strip().split('\t')
        
        if len(header) < 3:
            print(f"Error: Invalid header format. Expected 3 columns, got {len(header)}", 
                  file=sys.stderr)
            sys.exit(1)
        
        if header[0] != 'original_file_path' or header[2] != 'archived_file_path':
            print(f"Warning: Unexpected header format. Expected columns:", file=sys.stderr)
            print(f"  [original_file_path, archive_directory, archived_file_path]", file=sys.stderr)
            print(f"Got: {header}", file=sys.stderr)
        
        # Read data rows
        for line_num, line in enumerate(f, start=2):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) < 3:
                print(f"Warning: Skipping malformed line {line_num}: {line}", file=sys.stderr)
                continue
            
            local_path = parts[0].strip()
            archive_dir = parts[1].strip()
            s3_path = parts[2].strip()
            
            mappings.append(FileMapping(
                local_path=local_path,
                s3_path=s3_path,
                archive_dir=archive_dir
            ))
    
    return mappings


def validate_local_file(local_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that a local file exists and is readable.
    
    Args:
        local_path: Path to the local file
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not os.path.exists(local_path):
        return False, f"File does not exist: {local_path}"
    
    if not os.path.isfile(local_path):
        return False, f"Path is not a file: {local_path}"
    
    if not os.access(local_path, os.R_OK):
        return False, f"File is not readable: {local_path}"
    
    return True, None


def parse_s3_path(s3_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse an S3 path into bucket and key.
    
    Args:
        s3_path: S3 path (can be s3://bucket/key or bucket/key format)
        
    Returns:
        Tuple of (bucket_name, key)
    """
    # Handle s3:// prefix
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    # Split into bucket and key
    parts = s3_path.split('/', 1)
    if len(parts) < 2:
        return None, None
    
    return parts[0], parts[1]


def build_s3_url(bucket: str, s3_key: str) -> str:
    """
    Build a full s3:// URL from bucket and key.
    
    Args:
        bucket: S3 bucket name
        s3_key: S3 object key
        
    Returns:
        Full S3 URL
    """
    return f"s3://{bucket}/{s3_key}"


def upload_file_to_s3(local_path: str, 
                      bucket: str, 
                      s3_key: str,
                      dry_run: bool = False,
                      verbose: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Upload a single file to S3 using s3cmd.
    
    Args:
        local_path: Path to the local file
        bucket: S3 bucket name
        s3_key: S3 object key
        dry_run: If True, don't actually upload
        verbose: If True, show s3cmd output
        
    Returns:
        Tuple of (success, error_message)
    """
    if dry_run:
        return True, None
    
    # Build s3cmd command
    s3_url = build_s3_url(bucket, s3_key)
    cmd = ['s3cmd', 'put', local_path, s3_url]
    
    try:
        if verbose:
            # Show s3cmd output
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=False,
                text=True
            )
        else:
            # Capture and suppress s3cmd output
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
        return True, None
        
    except subprocess.CalledProcessError as e:
        error_msg = f"s3cmd failed (exit code {e.returncode})"
        if e.stderr:
            error_msg += f": {e.stderr.strip()}"
        return False, error_msg
    except FileNotFoundError:
        return False, "s3cmd command not found"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def upload_worker(args: Tuple[FileMapping, str, bool, bool, bool]) -> Tuple[FileMapping, bool, Optional[str]]:
    """
    Worker function for parallel uploads.
    
    Args:
        args: Tuple of (mapping, bucket_name, dry_run, validation_only, verbose)
        
    Returns:
        Tuple of (mapping, success, error_message)
    """
    mapping, bucket_name, dry_run, validation_only, verbose = args
    
    # Validate local file
    is_valid, error_msg = validate_local_file(mapping.local_path)
    if not is_valid:
        return mapping, False, error_msg
    
    # Parse S3 path
    bucket, s3_key = parse_s3_path(mapping.s3_path)
    
    # Override bucket if provided
    if bucket_name:
        bucket = bucket_name
    elif not bucket:
        return mapping, False, f"Cannot determine S3 bucket from path: {mapping.s3_path}"
    
    # If validation only, stop here
    if validation_only:
        return mapping, True, None
    
    # Upload file using s3cmd
    success, error_msg = upload_file_to_s3(
        mapping.local_path, 
        bucket, 
        s3_key,
        dry_run,
        verbose
    )
    
    return mapping, success, error_msg


def sync_files(mappings: List[FileMapping],
               bucket_name: Optional[str] = None,
               dry_run: bool = False,
               num_workers: int = 4,
               validation_only: bool = False,
               verbose: bool = False) -> Tuple[int, int]:
    """
    Sync files to S3 in parallel using s3cmd.
    
    Args:
        mappings: List of FileMapping objects
        bucket_name: Optional bucket name to override paths
        dry_run: If True, don't actually upload
        num_workers: Number of parallel workers
        validation_only: If True, only validate files without uploading
        verbose: If True, show s3cmd output
        
    Returns:
        Tuple of (successful_count, failed_count)
    """
    successful = 0
    failed = 0
    completed = 0
    total = len(mappings)
    
    # Prepare arguments for workers
    worker_args = [(m, bucket_name, dry_run, validation_only, verbose) for m in mappings]
    
    # Process uploads in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(upload_worker, arg): arg[0] for arg in worker_args}
        
        for future in as_completed(futures):
            mapping = futures[future]
            completed += 1
            
            try:
                result_mapping, success, error_msg = future.result()
                
                if success:
                    successful += 1
                    if validation_only:
                        print(f"✓ Valid ({completed}/{total}): {result_mapping.local_path}")
                    elif dry_run:
                        # Parse S3 path for display
                        bucket, s3_key = parse_s3_path(result_mapping.s3_path)
                        if bucket_name:
                            bucket = bucket_name
                        s3_url = build_s3_url(bucket, s3_key)
                        print(f"✓ [DRY RUN] Would upload ({completed}/{total}): {result_mapping.local_path} -> {s3_url}")
                    else:
                        # Parse S3 path for display
                        bucket, s3_key = parse_s3_path(result_mapping.s3_path)
                        if bucket_name:
                            bucket = bucket_name
                        s3_url = build_s3_url(bucket, s3_key)
                        print(f"✓ Uploaded ({completed}/{total}): {result_mapping.local_path} -> {s3_url}")
                else:
                    failed += 1
                    print(f"✗ Failed ({completed}/{total}): {result_mapping.local_path}", file=sys.stderr)
                    if error_msg:
                        print(f"  Error: {error_msg}", file=sys.stderr)
                
                # Print progress summary every 10 files
                if completed % 5 == 0 and completed < total:
                    print(f"--- Progress: {completed}/{total} files processed ({successful} successful, {failed} failed) ---")
                        
            except Exception as e:
                failed += 1
                print(f"✗ Failed ({completed}/{total}): {mapping.local_path}", file=sys.stderr)
                print(f"  Error: {e}", file=sys.stderr)
                
                # Print progress summary every 10 files
                if completed % 5 == 0 and completed < total:
                    print(f"--- Progress: {completed}/{total} files processed ({successful} successful, {failed} failed) ---")
    
    return successful, failed


def main():
    parser = argparse.ArgumentParser(
        description='Sync files to S3 based on archive mapping file using s3cmd.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be uploaded
  %(prog)s archive_mapping.txt --dry-run
  
  # Upload files to S3
  %(prog)s archive_mapping.txt
  
  # Override bucket name for all files
  %(prog)s archive_mapping.txt --bucket my-custom-bucket
  
  # Use 8 parallel workers
  %(prog)s archive_mapping.txt --workers 8
  
  # Validate files only (don't upload)
  %(prog)s archive_mapping.txt --validate-only
  
  # Combine dry run with custom bucket
  %(prog)s archive_mapping.txt --bucket my-bucket --dry-run --workers 10
  
  # Show s3cmd output (verbose mode)
  %(prog)s archive_mapping.txt --verbose

Requirements:
  - s3cmd must be installed and configured
  - Run 's3cmd --configure' to set up AWS credentials
        """
    )
    
    parser.add_argument(
        'mapping_file',
        type=Path,
        help='Archive mapping file (TSV output from archive_organizer.py)'
    )
    
    parser.add_argument(
        '-b', '--bucket',
        help='S3 bucket name (overrides bucket in archived_file_path column)'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=4,
        help='Number of parallel upload workers (default: 4)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be uploaded without actually uploading'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate that local files exist and are readable'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Print verbose output including s3cmd output'
    )
    
    args = parser.parse_args()
    
    # Check if s3cmd is installed
    s3cmd_available, s3cmd_path = check_s3cmd_installed()
    if not s3cmd_available:
        print("\nError: s3cmd is not installed or not found in PATH!", file=sys.stderr)
        sys.exit(1)
    
    if args.verbose:
        print(f"Using s3cmd: {s3cmd_path}")
    
    # Validate mapping file exists
    if not args.mapping_file.exists():
        print(f"Error: Mapping file not found: {args.mapping_file}", file=sys.stderr)
        sys.exit(1)
    
    # Load mappings
    if args.verbose:
        print(f"Loading mappings from: {args.mapping_file}")
    
    mappings = load_mapping_file(args.mapping_file)
    
    if not mappings:
        print("Error: No file mappings found in input file", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loaded {len(mappings)} file mappings")
    
    # Print configuration
    if args.dry_run:
        print("\n[DRY RUN MODE - No files will be uploaded]")
    if args.validate_only:
        print("\n[VALIDATION MODE - Only checking local files]")
    if args.bucket:
        print(f"Using bucket: {args.bucket}")
    print(f"Parallel workers: {args.workers}")
    print("")
    
    # Sync files
    successful, failed = sync_files(
        mappings=mappings,
        bucket_name=args.bucket,
        dry_run=args.dry_run,
        num_workers=args.workers,
        validation_only=args.validate_only,
        verbose=args.verbose
    )
    
    # Print summary
    print("\n" + "=" * 80)
    print("SYNC SUMMARY")
    print("=" * 80)
    print(f"Total files:     {len(mappings)}")
    print(f"Successful:      {successful}")
    print(f"Failed:          {failed}")
    
    if args.dry_run:
        print("\n[DRY RUN - No actual uploads performed]")
    elif args.validate_only:
        print("\n[VALIDATION COMPLETE]")
    
    # Exit with error code if any uploads failed
    if failed > 0:
        sys.exit(1)
    else:
        print("\n✓ All files processed successfully")
        sys.exit(0)


if __name__ == '__main__':
    main()
