import os
import re
import sys
import argparse

def amalgamate(root_dir):
    # Matches local includes
    include_pattern = re.compile(r'^\s*#\s*include\s+"(.+?)"')
    # Matches #pragma once (case insensitive)
    pragma_pattern = re.compile(r'^\s*#\s*pragma\s+once\s*', re.IGNORECASE)
    
    processed_files = set()

    def process_file(file_path):
        abs_path = os.path.abspath(file_path)
        
        if abs_path in processed_files:
            return
        
        if not os.path.exists(abs_path):
            print(f"/* Warning: Could not find {file_path} */", file=sys.stderr)
            return

        # Read lines first to scan for dependencies
        try:
            with open(abs_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"/* Error reading {file_path}: {e} */", file=sys.stderr)
            return

        # --- PHASE 1: Dependency Resolution (Post-Order DFS) ---
        # We look for all local includes and process them BEFORE printing this file.
        # This ensures that if foo depends on bar, bar is printed first.
        for line in lines:
            match = include_pattern.match(line)
            if match:
                header_name = match.group(1)
                header_path = os.path.join(os.path.dirname(abs_path), header_name)
                if os.path.exists(header_path):
                    process_file(header_path)

        # Mark as processed so we don't print it again if another file includes it
        if abs_path in processed_files:
            return
        processed_files.add(abs_path)

        # --- PHASE 2: Output Content ---
        print(f"\n/* --- Start of {os.path.basename(abs_path)} --- */")
        
        for line in lines:
            # Skip #pragma once
            if pragma_pattern.match(line):
                continue
            
            # Skip the local include lines themselves (since we've already expanded them)
            if include_pattern.match(line):
                # Optionally replace with a comment trace
                match = include_pattern.match(line)
                print(f"/* Expanded: {match.group(1)} */")
                continue
                
            sys.stdout.write(line)
            
        print(f"/* --- End of {os.path.basename(abs_path)} --- */")

    # Entry Point: Scan for all headers in the directory
    valid_extensions = ('.h', '.hpp', '.hh', '.hxx')
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(valid_extensions):
                process_file(os.path.join(root, file))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ordered Amalgamator (Post-Order Traversal)")
    parser.add_argument("path", nargs="?", default=".", help="Directory to scan")
    args = parser.parse_args()

    if os.path.isdir(args.path):
        amalgamate(args.path)
    else:
        print(f"Error: {args.path} is not a directory.", file=sys.stderr)
