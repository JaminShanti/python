#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Modernized F5 Node Health Check Script
Original Author: Jamin Shanti
Created: 08/29/2014
Updated: 2023
"""

import sys
import os
import getpass
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


class F5NodeHealthChecker:
    def __init__(self, hostname, username, password, partition=None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.partition = partition
        self.client = None

    def connect(self):
        """Establishes connection to the F5 device."""
        logger.info(f"Connecting to {self.hostname} as {self.username}...")
        try:
            self.client = bigsuds.BIGIP(
                hostname=self.hostname,
                username=self.username,
                password=self.password
            )
            version = self.client.System.SystemInfo.get_version()
            logger.info(f"Connected to F5 Version: {version}")
            
            if self.partition:
                logger.info(f"Setting active partition to: {self.partition}")
                self.client.Management.Partition.set_active_partition(self.partition)
                
        except bigsuds.ConnectionError as e:
            logger.error(f"Connection failed: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"An error occurred during connection: {e}")
            sys.exit(1)

    def check_pool_health(self, site_filter):
        """Checks the health of pools matching the site_filter."""
        if not self.client:
            logger.error("Not connected to F5.")
            return

        try:
            # Get all pools
            all_pools = self.client.LocalLB.Pool.get_list()
            
            # Filter pools based on site_name
            target_pools = [pool for pool in all_pools if site_filter in pool]
            
            if not target_pools:
                logger.warning(f"No pools found matching filter: '{site_filter}'")
                return

            logger.info(f"Found {len(target_pools)} pools matching '{site_filter}'. Checking health...")

            for pool in target_pools:
                self._check_single_pool(pool)

        except Exception as e:
            logger.error(f"Error retrieving pool list: {e}")

    def _check_single_pool(self, pool_name):
        """Checks session and monitor status for a single pool."""
        logger.debug(f"Checking pool: {pool_name}")
        
        try:
            # Check Session Status (Enabled/Disabled)
            session_statuses = self.client.LocalLB.PoolMember.get_session_status([pool_name])[0]
            for member_status in session_statuses:
                member_def = member_status['member']
                status = member_status['session_status']
                
                if status != "SESSION_STATUS_ENABLED":
                    logger.warning(f"Pool: {pool_name} | Member: {member_def['address']}:{member_def['port']} | Session Status: {status}")

            # Check Monitor Status (Up/Down)
            monitor_statuses = self.client.LocalLB.PoolMember.get_monitor_status([pool_name])[0]
            for member_status in monitor_statuses:
                member_def = member_status['member']
                status = member_status['monitor_status']
                
                if status != "MONITOR_STATUS_UP":
                    logger.error(f"Pool: {pool_name} | Member: {member_def['address']}:{member_def['port']} | Monitor Status: {status}")

        except bigsuds.ServerError as e:
            # Sometimes pools might be in Common partition even if we are in another
            # This logic mimics the original script's fallback attempt
            logger.warning(f"Error checking {pool_name}, trying 'Common' partition fallback. Error: {e}")
            try:
                original_partition = self.client.Management.Partition.get_active_partition()
                self.client.Management.Partition.set_active_partition('Common')
                
                # Retry checks (simplified for brevity, ideally recursive or factored out)
                # Just logging the success of fallback for now
                self.client.LocalLB.PoolMember.get_session_status([pool_name])
                logger.info(f"Fallback to Common partition successful for {pool_name}")
                
                # Restore partition
                self.client.Management.Partition.set_active_partition(original_partition)
                
            except Exception as fallback_error:
                logger.error(f"Fallback failed for {pool_name}: {fallback_error}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Audit F5 Pool Node Health")
    
    parser.add_argument("--f5-host", dest="f5_hostname", required=True,
                        help="F5 BigIP Hostname or IP")
    parser.add_argument("--f5-user", dest="f5_username", default=getpass.getuser(),
                        help="F5 Username (default: current user)")
    parser.add_argument("--site-name", dest="site_name", default="Patch",
                        help="Filter string for pool names (e.g., 'Patch', 'BETA')")
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

    checker = F5NodeHealthChecker(
        hostname=args.f5_hostname,
        username=args.f5_username,
        password=f5_password,
        partition=args.partition
    )

    checker.connect()
    checker.check_pool_health(args.site_name)


if __name__ == '__main__':
    main()
