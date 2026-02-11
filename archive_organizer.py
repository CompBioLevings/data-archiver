#!/usr/bin/env python3
"""
Generate context-specific archive directory names for FASTQ sequencing data.
"""

import argparse
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Extract metadata from file path
def extract_metadata(filepath: str) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Extract metadata components from file path.
    
    Args:
        filepath: Full path to the file
        
    Returns:
        Tuple of (researcher, experiment_type, description, base_fastq_dir)
    """
    parts = filepath.split('/')
    
    # Find the data directory level (after /data/ or /external_data/)
    try:
        if '/data/' in filepath:
            data_idx = parts.index('data')
            remaining_path = '/'.join(parts[data_idx + 1:])
        elif '/external_data/' in filepath:
            data_idx = parts.index('external_data')
            remaining_path = '/'.join(parts[data_idx + 1:])
        else:
            return None, None, None, filepath
    except ValueError:
        return None, None, None, filepath
    
    # Get researcher/project name (first directory after /data/)
    researcher = parts[data_idx + 1] if data_idx + 1 < len(parts) else None
    
    # Remove ChIP-seq or RNA-seq suffixes from researcher name
    if researcher:
        researcher = re.sub(r'[\-\_]?((RNA)|(ChIP)|(ATAC))\-?(seq)?$', '', researcher, flags=re.IGNORECASE)
    
    # Get experiment directory (second directory after /data/)
    experiment_dir = parts[data_idx + 2] if data_idx + 2 < len(parts) else None
    
    # Extract experiment type and description from experiment directory
    experiment_type: Optional[str] = None
    description: Optional[str] = None
    
    if experiment_dir:
        # Parse patterns like "Salil_RNA-seq", "mm10_aged-ARS-CRS_and_RES-SUS_20240102"
        # Remove researcher name prefix if present
        exp_cleaned = experiment_dir
        if researcher and exp_cleaned.startswith(researcher + '_'):
            exp_cleaned = exp_cleaned[len(researcher) + 1:]
        elif researcher and exp_cleaned.startswith(researcher + '-'):
            exp_cleaned = exp_cleaned[len(researcher) + 1:]
        
        # Try to extract experiment type (RNA-seq, ChIP-seq, etc.)
        seq_match = re.search(r'((RNA)|(ChIP)|(ATAC))', exp_cleaned, re.IGNORECASE)
        if seq_match:
            experiment_type = seq_match.group(1) + '-seq'
            # Everything else is description
            description = exp_cleaned
        # Next try to find the patterns anywhere in the file path- prioritize the last match in the path
        elif re.search(r'((RNA)|(ChIP)|(ATAC))', remaining_path, re.IGNORECASE):
            all_matches = re.findall(r'((RNA)|(ChIP)|(ATAC))', remaining_path, re.IGNORECASE)
            seq_match = all_matches[-1][0]  # Get the last full match
            experiment_type = seq_match + '-seq'
            description = exp_cleaned
        else:
            # No clear experiment type, use whole thing as description
            description = exp_cleaned
    
    # Check if the description is the actual sample name, and if so 'clear it'
    sample_name_pattern = re.compile(r'.*\.fastq\.?g?z?$', re.IGNORECASE)
    if sample_name_pattern.match(description):
        description = None
    
    # Get the base FASTQ directory (everything up to and including FASTQ/STAR/bbsplit)
    fastq_idx: Optional[int] = None
    for i, part in enumerate(parts):
        if part in ['FASTQ', 'fastq']:
            fastq_idx = i
            break
    
    if fastq_idx:
        base_fastq_dir = '/'.join(parts[:fastq_idx + 1])
    else:
        # If no FASTQ directory found, use the directory of the file
        base_fastq_dir = os.path.dirname(filepath)
    
    return researcher, experiment_type, description, base_fastq_dir

# Function to propose archive directory name based on parsed metadata
def propose_archive_directory(researcher: Optional[str], 
                              experiment_type: Optional[str], 
                              description: Optional[str]) -> str:
    """
    Propose an archive directory name based on metadata.
    
    Args:
        researcher: Researcher or project name
        experiment_type: Type of experiment (RNA-seq, ChIP-seq, etc.)
        description: Additional description or context
        
    Returns:
        Proposed archive directory path
    """
    
    # Now use match-case to convert experiment type to one that will be compatible with s3 bucket naming conventions
    if experiment_type:
        match experiment_type.lower():
            case 'rna-seq':
                experiment_type = 'slattery-rnaseq'
            case 'chip-seq':
                experiment_type = 'slattery-chipseq'
            case 'atac-seq':
                experiment_type = 'slattery-atacseq'
            case _:
                # For any other experiment types, set to None
                experiment_type = None

    components: List[str] = []
    
    if experiment_type:
        components.append(experiment_type)

    if researcher:
        components.append(researcher)
    
    if description:
        # Clean up description
        desc_clean = description
        # Remove organism prefixes like mm10_, hg38_
        desc_clean = re.sub(r'^((mm)|(hg)|(dm))\d+_', '', desc_clean)
        # Remove experiment type if already captured
        if experiment_type:
            desc_clean = re.sub(r'((RNA)|(ChIP)|(ATAC))\-?(seq)?_?', '', desc_clean, flags=re.IGNORECASE)
        # Remove leading/trailing underscores and hyphens
        desc_clean = desc_clean.strip('_-')
        
        if desc_clean:
            components.append(desc_clean)
    
    return '/'.join(components) if components else 'uncategorized'

# Function to group files by their FASTQ directory
def group_files_by_fastq_dir(filepaths: List[str]) -> Dict[str, List[str]]:
    """
    Group files by their common FASTQ directory.
    
    Args:
        filepaths: List of file paths
        
    Returns:
        Dictionary mapping base_fastq_dir to list of files
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    
    for filepath in filepaths:
        filepath = filepath.strip()
        if not filepath:
            continue
        
        _, _, _, base_fastq_dir = extract_metadata(filepath)
        groups[base_fastq_dir].append(filepath)
    
    return groups

# Function to get user input for interactive review of proposed archive directories
def interactive_review(archive_proposals: Dict[str, Tuple[str, List[str]]], 
                      ignore_file_path: str = 'ignore-files.txt') -> Dict[str, str]:
    """
    Allow user to review and modify proposed archive directory names.
    
    Args:
        archive_proposals: Dictionary mapping base_fastq_dir to (proposed_dir, file_list)
        ignore_file_path: Path to file where skipped files will be appended
        
    Returns:
        Dictionary mapping base_fastq_dir to final archive directory name
    """
    final_mapping: Dict[str, str] = {}
    
    print("\n" + "=" * 80)
    print("INTERACTIVE REVIEW OF PROPOSED ARCHIVE DIRECTORIES")
    print("=" * 80)
    print("\nFor each proposed archive directory, you can:")
    print("  - Press ENTER to accept the proposal")
    print("  - Type a new name to replace it or type 'skip' to not include it in the archive output")
    print("=" * 80 + "\n")
    
    for i, (base_fastq_dir, (proposed_dir, file_list)) in enumerate(archive_proposals.items(), 1):
        print(f"\n[{i}/{len(archive_proposals)}]")
        print(f"Proposed archive directory: {proposed_dir}")
        print(f"Number of files: {len(file_list)}")
        print(f"Example files (up to 3):")
        for example_file in file_list[:3]:
            filename = os.path.basename(example_file)
            print(f"  - {filename}")
        if len(file_list) > 3:
            print(f"  ... and {len(file_list) - 3} more")
        
        print(f"\nOriginal FASTQ directory: {base_fastq_dir}")
        
        user_input = input(f"\nAccept '{proposed_dir}', enter 'skip' (with no quotes) to exclude, or enter new name: ").strip()
        
        if user_input:
            if user_input.lower() == 'skip':
                # Append skipped files to ignore file
                with open(ignore_file_path, 'a') as ignore_f:
                    for filepath in file_list:
                        ignore_f.write(filepath.strip() + '\n')
                print(f"\n✓ Skipped: {base_fastq_dir} will not be included in the archive output")
                print(f"  {len(file_list)} file(s) appended to {ignore_file_path}\n")
                continue
            final_mapping[base_fastq_dir] = user_input
            print(f"✓ Updated to: {user_input}")
        else:
            final_mapping[base_fastq_dir] = proposed_dir
            print(f"✓ Accepted: {proposed_dir}")
    
    print("\n" + "=" * 80)
    print("REVIEW COMPLETE")
    print("=" * 80 + "\n")
    
    return final_mapping

# function to generate output table
def generate_output_table(file_groups: Dict[str, List[str]], 
                         final_archive_dirs: Dict[str, str], 
                         output_file: str) -> None:
    """
    Generate tab-delimited output file with original paths and proposed archive paths.
    
    Args:
        file_groups: Dictionary mapping base_fastq_dir to list of files
        final_archive_dirs: Dictionary mapping base_fastq_dir to archive directory name
        output_file: Path to output file
    """
    with open(output_file, 'w') as f:
        # Write header
        f.write("original_file_path\tarchive_directory\tarchived_file_path\n")
        
        # Write data rows
        for base_fastq_dir, file_list in file_groups.items():
            # Skip directories that were not included in final mapping (i.e., marked as 'skip')
            if base_fastq_dir not in final_archive_dirs:
                continue
            
            archive_dir = final_archive_dirs[base_fastq_dir]
            
            for filepath in sorted(file_list):
                filename = os.path.basename(filepath)
                archived_path = f"{archive_dir}/{filename}"
                
                f.write(f"{filepath}\t{archive_dir}\t{archived_path}\n")
    
    print(f"Output table written to: {output_file}")

# main function
def main() -> None:
    """Main entry point for the script."""
    example_text = '''Example usage:
    
    archive_organizer.py input_fastq_list.txt -o fastq_archive_mapping.txt
    '''
    parser = argparse.ArgumentParser(
        description='Generate context-specific archive directory names for FASTQ sequencing data.',
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'input_file',
        help='Input file containing file paths (one per line)'
    )
    parser.add_argument(
        '-o', '--output',
        default='archive_mapping.txt',
        help='Output file for archive mapping table (default: archive_mapping.txt)'
    )
    parser.add_argument(
        '--no-interactive',
        action='store_true',
        help='Skip interactive review and use proposed names directly'
    )
    parser.add_argument(
        '--ignore-file',
        default='ignore-files.txt',
        help='File to append skipped files to (default: ignore-files.txt)'
    )
    
    args = parser.parse_args()
    
    # Read input file
    print(f"Reading file paths from: {args.input_file}")
    with open(args.input_file, 'r') as f:
        filepaths: List[str] = f.readlines()
    
    print(f"Found {len(filepaths)} file paths")
    
    # Group files by FASTQ directory
    file_groups = group_files_by_fastq_dir(filepaths)
    print(f"Grouped into {len(file_groups)} unique FASTQ directories")
    
    # Generate proposed archive directory names
    archive_proposals: Dict[str, Tuple[str, List[str]]] = {}
    for base_fastq_dir, file_list in file_groups.items():
        researcher, experiment_type, description, _ = extract_metadata(file_list[0])
        proposed_dir = propose_archive_directory(researcher, experiment_type, description)
        archive_proposals[base_fastq_dir] = (proposed_dir, file_list)
    
    # Interactive review or auto-accept
    if args.no_interactive:
        final_archive_dirs: Dict[str, str] = {k: v[0] for k, v in archive_proposals.items()}
        print("Skipping interactive review (--no-interactive flag)")
    else:
        final_archive_dirs = interactive_review(archive_proposals, args.ignore_file)
    
    # Generate output table
    generate_output_table(file_groups, final_archive_dirs, args.output)
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Total files: {sum(len(files) for files in file_groups.values())}")
    print(f"  Unique archive directories: {len(set(final_archive_dirs.values()))}")
    print(f"  Output file: {args.output}")

# run it
if __name__ == '__main__':
    main()