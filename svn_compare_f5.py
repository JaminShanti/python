#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Modernized SVN vs F5 iRule Comparison Script
Original Author: Jamin Shanti
Created: 08/25/2014
Updated: 2023
"""

import sys
import os
import getpass
import subprocess
import difflib
import logging
import argparse
from datetime import datetime

# Try importing bigsuds, handle if missing
try:
    import bigsuds
except ImportError:
    print("Error: 'bigsuds' module not found. Please install it using 'pip install bigsuds'.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{os.path.splitext(os.path.basename(__file__))[0]}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy suds logging
logging.getLogger('suds.client').setLevel(logging.WARNING)
logging.getLogger('suds.transport').setLevel(logging.WARNING)
logging.getLogger('suds.xsd.schema').setLevel(logging.WARNING)
logging.getLogger('suds.wsdl').setLevel(logging.WARNING)


class F5SvnComparator:
    def __init__(self, f5_hostname, f5_username, f5_password, svn_url, partition='Common'):
        self.f5_hostname = f5_hostname
        self.f5_username = f5_username
        self.f5_password = f5_password
        self.svn_url = svn_url.rstrip('/')
        self.partition = partition
        self.svn_files = {}  # Dictionary to store {filename: content}
        self.f5_rules = {}   # Dictionary to store {rule_name: content}

    def fetch_svn_files(self):
        """
        Lists and fetches .tcl files from the SVN repository.
        """
        logger.info(f"Fetching file list from SVN: {self.svn_url}")
        
        try:
            # List files
            cmd_ls = ["svn", "ls", self.svn_url]
            result_ls = subprocess.run(cmd_ls, capture_output=True, text=True, check=True)
            files = [f.strip() for f in result_ls.stdout.splitlines() if f.strip().endswith('.tcl')]
            
            if not files:
                logger.warning("No .tcl files found in the specified SVN location.")
                return

            logger.info(f"Found {len(files)} .tcl files in SVN.")

            # Fetch content for each file
            for filename in files:
                file_url = f"{self.svn_url}/{filename}"
                logger.debug(f"Fetching content for {filename}")
                
                cmd_cat = ["svn", "cat", file_url]
                result_cat = subprocess.run(cmd_cat, capture_output=True, text=True, check=True)
                
                # Normalize line endings to avoid false positives in diff
                content = result_cat.stdout.replace('\r\n', '\n').strip()
                self.svn_files[filename] = content
                
        except subprocess.CalledProcessError as e:
            logger.error(f"SVN command failed: {e}")
            logger.error(f"Stderr: {e.stderr}")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("SVN client not found. Please ensure 'svn' is in your PATH.")
            sys.exit(1)

    def fetch_f5_rules(self):
        """
        Connects to F5 and fetches iRules corresponding to the SVN files.
        """
        logger.info(f"Connecting to F5: {self.f5_hostname} as {self.f5_username}")
        
        try:
            b = bigsuds.BIGIP(
                hostname=self.f5_hostname, 
                username=self.f5_username, 
                password=self.f5_password
            )
            
            # Set partition
            logger.info(f"Setting active partition to: {self.partition}")
            b.Management.Partition.set_active_partition(self.partition)
            
            # Get version just to verify connection
            version = b.System.SystemInfo.get_version()
            logger.info(f"Connected to F5 Version: {version}")

            # Fetch rules
            # We only look for rules that match the SVN filenames (minus extension)
            rule_names_to_query = [f.replace('.tcl', '') for f in self.svn_files.keys()]
            
            if not rule_names_to_query:
                return

            # Query rules one by one to handle missing ones gracefully
            # (Bulk query might fail if one is missing, depending on API version)
            for rule_name in rule_names_to_query:
                try:
                    # query_rule returns a list of RuleDefinition structs
                    rule_def = b.LocalLB.Rule.query_rule([rule_name])[0]
                    content = rule_def['rule_definition'].replace('\r\n', '\n').strip()
                    
                    # F5 might prepend "definition {\n" and append "\n}" or similar wrappers
                    # For accurate diff, we might need to strip the definition wrapper if SVN has raw code.
                    # However, usually iRules in SVN include the 'when EVENT {' blocks.
                    # The 'rule_definition' from API usually contains the full text.
                    
                    self.f5_rules[rule_name] = content
                except Exception as e:
                    logger.warning(f"Rule '{rule_name}' not found on F5 or error retrieving: {e}")
                    self.f5_rules[rule_name] = None

        except Exception as e:
            logger.error(f"F5 Connection/API Error: {e}")
            sys.exit(1)

    def compare_and_report(self):
        """
        Compares SVN content with F5 content and prints a report/diff.
        """
        logger.info("Starting comparison...")
        print("\n" + "="*60)
        print(f"COMPARISON REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")

        matches = 0
        mismatches = 0
        missing = 0

        for filename, svn_content in self.svn_files.items():
            rule_name = filename.replace('.tcl', '')
            f5_content = self.f5_rules.get(rule_name)

            print(f"Checking: {rule_name} ({filename})")

            if f5_content is None:
                print(f"  [MISSING] Rule '{rule_name}' exists in SVN but NOT on F5.")
                missing += 1
                continue

            # Normalize whitespace for comparison (optional, but recommended)
            # Here we do a strict string compare first
            if svn_content == f5_content:
                print(f"  [MATCH]   Content is identical.")
                matches += 1
            else:
                print(f"  [DIFF]    Content differs!")
                mismatches += 1
                
                # Generate Diff
                svn_lines = svn_content.splitlines()
                f5_lines = f5_content.splitlines()
                
                diff = difflib.unified_diff(
                    f5_lines, 
                    svn_lines, 
                    fromfile=f'F5:{rule_name}', 
                    tofile=f'SVN:{filename}',
                    lineterm=''
                )
                
                print("\n  --- Diff Start ---")
                for line in diff:
                    print("  " + line)
                print("  --- Diff End ---\n")

        print("\n" + "="*60)
        print(f"SUMMARY: Matches: {matches} | Mismatches: {mismatches} | Missing on F5: {missing}")
        print("="*60 + "\n")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Compare F5 iRules with SVN Repository")
    
    parser.add_argument("--f5-host", dest="f5_hostname", required=True,
                        help="F5 BigIP Hostname or IP")
    parser.add_argument("--f5-user", dest="f5_username", default=getpass.getuser(),
                        help="F5 Username (default: current user)")
    parser.add_argument("--svn-url", dest="svn_url", required=True,
                        help="URL to SVN directory containing .tcl files")
    parser.add_argument("--partition", dest="partition", default="Common",
                        help="F5 Partition to query (default: Common)")
    parser.add_argument("--password", dest="password", help="F5 Password (if not provided, will prompt)")

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Handle Password
    f5_password = args.password
    if not f5_password:
        try:
            f5_password = getpass.getpass(f"Enter password for {args.f5_username}@{args.f5_hostname}: ")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            sys.exit(0)

    comparator = F5SvnComparator(
        f5_hostname=args.f5_hostname,
        f5_username=args.f5_username,
        f5_password=f5_password,
        svn_url=args.svn_url,
        partition=args.partition
    )

    comparator.fetch_svn_files()
    comparator.fetch_f5_rules()
    comparator.compare_and_report()


if __name__ == '__main__':
    main()
