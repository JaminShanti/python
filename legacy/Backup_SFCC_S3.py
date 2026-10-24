import argparse
import logging
import sys
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
import boto3
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("BackupSFCC")

class BackupSFCCInstance:
    """
    Backup SFCC instance files from WebDAV to S3.
    """

    def __init__(self):
        self.webdav_hostname = ""
        self.webdav_path = "/on/demandware.servlet/webdav/Sites/Impex/src/instance/"
        self.webdav_user = None
        self.webdav_password = None
        self.webdav_filetype = "application/zip"
        self.s3_bucket = ""
        self.s3_prefix = ""
        self.s3_client = boto3.client("s3")

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description="Backup SFCC WebDAV files to S3")
        parser.add_argument("--webDavHostname", "-H", dest="webDavHostname", required=True, help="WebDAV host (e.g., webdav.example.com)")
        parser.add_argument("--webDavPath", "-p", dest="webDavPath", default=self.webdav_path, help="Path on WebDAV server")
        parser.add_argument("--webDavFileType", "-T", dest="webDavFileType", default=self.webdav_filetype, help="File type to filter (e.g., application/zip)")
        parser.add_argument("--webDAVuserID", "-U", dest="webDAVuserID", required=True, help="WebDAV Username")
        parser.add_argument("--webDAVpassword", "-P", dest="webDAVpassword", required=True, help="WebDAV Password")
        parser.add_argument("--S3BucketName", "-B", dest="S3BucketName", required=True, help="Target S3 Bucket Name")
        parser.add_argument("--S3BucketPath", "-d", dest="S3BucketPath", default="", help="Target Path/Prefix in S3 Bucket")
        parser.add_argument("--verbose", "-v", action="store_true", dest="verbose", help="Enable verbose logging")

        args = parser.parse_args()

        if args.verbose:
            logger.setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")

        self.webdav_hostname = args.webDavHostname
        if not self.webdav_hostname.startswith("http"):
            self.webdav_hostname = "https://" + self.webdav_hostname

        self.webdav_path = args.webDavPath
        self.webdav_filetype = args.webDavFileType
        self.webdav_user = args.webDAVuserID
        self.webdav_password = args.webDAVpassword
        self.s3_bucket = args.S3BucketName
        self.s3_prefix = args.S3BucketPath

        # Normalize S3 prefix
        if self.s3_prefix and not self.s3_prefix.endswith("/"):
            self.s3_prefix += "/"

        logger.debug(f"Configuration: Host={self.webdav_hostname}, Path={self.webdav_path}, Type={self.webdav_filetype}, Bucket={self.s3_bucket}, Prefix={self.s3_prefix}")

    def list_s3_objects(self) -> set:
        """
        Returns a set of filenames already present in S3 under the specified prefix.
        """
        existing_files = set()
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        # Extract filename from key
                        key = obj["Key"]
                        filename = os.path.basename(key)
                        if filename: # Ignore "directories"
                            existing_files.add(filename)
            
            logger.info(f"Found {len(existing_files)} existing files in S3 bucket '{self.s3_bucket}' under prefix '{self.s3_prefix}'")
            return existing_files
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            sys.exit(1)

    def list_webdav_files(self) -> List[Dict]:
        """
        Performs a PROPFIND request to list files in the WebDAV directory.
        """
        full_url = urljoin(self.webdav_hostname, self.webdav_path)
        logger.info(f"Querying WebDAV: {full_url}")

        headers = {"Depth": "1"}
        # PROPFIND body to request specific properties
        body = """<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:resourcetype/>
    <D:getcontenttype/>
    <D:getcontentlength/>
    <D:getlastmodified/>
  </D:prop>
</D:propfind>"""

        try:
            response = requests.request(
                "PROPFIND", 
                full_url, 
                data=body, 
                headers=headers, 
                auth=HTTPBasicAuth(self.webdav_user, self.webdav_password),
                timeout=30
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"WebDAV request failed: {e}")
            sys.exit(1)

        files = []
        try:
            # Parse XML response
            # Namespaces can be tricky, usually 'DAV:' is the default
            # We'll use a namespace map for findall
            namespaces = {'d': 'DAV:'}
            root = ET.fromstring(response.content)

            for response_node in root.findall('d:response', namespaces):
                href_node = response_node.find('d:href', namespaces)
                if href_node is None:
                    continue
                
                href = href_node.text
                
                propstat_node = response_node.find('d:propstat', namespaces)
                if propstat_node:
                    prop_node = propstat_node.find('d:prop', namespaces)
                    if prop_node:
                        # Check if it is a collection (directory)
                        resourcetype = prop_node.find('d:resourcetype', namespaces)
                        if resourcetype is not None and resourcetype.find('d:collection', namespaces) is not None:
                            continue # Skip directories

                        content_type_node = prop_node.find('d:getcontenttype', namespaces)
                        content_type = content_type_node.text if content_type_node is not None else ""
                        
                        # Extract filename from href
                        filename = os.path.basename(href.rstrip('/'))
                        
                        if not filename:
                            continue

                        files.append({
                            'name': filename,
                            'href': href,
                            'content_type': content_type
                        })

            logger.info(f"Found {len(files)} files on WebDAV.")
            return files

        except ET.ParseError as e:
            logger.error(f"Failed to parse WebDAV XML response: {e}")
            sys.exit(1)

    def download_file(self, href: str, local_path: Path):
        """
        Downloads a file from WebDAV to a local path.
        """
        # Construct full URL. href might be absolute path from root, not full URL
        if href.startswith("http"):
            url = href
        else:
            url = urljoin(self.webdav_hostname, href)

        logger.debug(f"Downloading {url} to {local_path}")

        try:
            with requests.get(url, stream=True, auth=HTTPBasicAuth(self.webdav_user, self.webdav_password), timeout=60) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    def upload_to_s3(self, local_path: Path, filename: str):
        """
        Uploads a local file to S3.
        """
        s3_key = f"{self.s3_prefix}{filename}"
        logger.info(f"Uploading {filename} to s3://{self.s3_bucket}/{s3_key}")
        
        try:
            self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise

    def run(self):
        self.parse_arguments()
        
        existing_s3_files = self.list_s3_objects()
        webdav_files = self.list_webdav_files()
        
        # Filter files by type if specified (simple substring check)
        # Note: WebDAV content-type might vary, so we can also check extension if needed
        # For now, relying on the user-provided filter matching the content-type returned by server
        files_to_process = [
            f for f in webdav_files 
            if self.webdav_filetype in (f['content_type'] or "") or self.webdav_filetype == "*"
        ]

        if not files_to_process:
            logger.info(f"No files matching type '{self.webdav_filetype}' found to process.")
            return

        logger.info(f"Processing {len(files_to_process)} files...")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            for file_info in files_to_process:
                filename = file_info['name']
                
                if filename in existing_s3_files:
                    logger.info(f"Skipping '{filename}' - already exists in S3.")
                    continue
                
                local_file_path = temp_path / filename
                
                try:
                    self.download_file(file_info['href'], local_file_path)
                    self.upload_to_s3(local_file_path, filename)
                    # File is automatically deleted when temp_dir is cleaned up, 
                    # but we can delete it explicitly to save space if processing many large files
                    local_file_path.unlink()
                except Exception as e:
                    logger.error(f"Failed to process '{filename}': {e}")
                    # Continue to next file

        logger.info("Backup operation completed.")

if __name__ == "__main__":
    backup_tool = BackupSFCCInstance()
    backup_tool.run()
