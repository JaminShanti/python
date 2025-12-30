# python
import argparse
import logging
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
import boto3
import requests
from requests.auth import HTTPBasicAuth


class BackupSFCCInstance:
    """
    Backup SFCC instance files from WebDAV to S3 (modernized).
    Usage example:
      python Backup_SFCC_S3.py --webDavHostname webdav.example.com --webDavPath /path/ --webDAVuserID user \
        --webDAVpassword pass --S3BucketName my-bucket --S3BucketPath backups/
    """

    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
        self.logger = logging.getLogger("BackupSFCC")
        self.webdav_hostname: str = ""
        self.webdav_path: str = "/on/demandware.servlet/webdav/Sites/Impex/src/instance/"
        self.webdav_user: Optional[str] = None
        self.webdav_password: Optional[str] = None
        self.webdav_filetype: str = "application/zip"
        self.s3_bucket: str = ""
        self.s3_prefix: str = ""
        self.verbose: bool = False
        self.s3_client = boto3.client("s3")

    def handle_options(self) -> int:
        parser = argparse.ArgumentParser(description="Backup SFCC WebDAV files to S3")
        parser.add_argument("--webDavHostname", "-H", dest="webDavHostname", help="WebDAV host (host or https://host)")
        parser.add_argument("--webDavPath", "-p", dest="webDavPath", default=self.webdav_path)
        parser.add_argument("--webDavFileType", "-T", dest="webDavFileType", default=self.webdav_filetype)
        parser.add_argument("--webDAVuserID", "-U", dest="webDAVuserID", default=None)
        parser.add_argument("--webDAVpassword", "-P", dest="webDAVpassword", default=None)
        parser.add_argument("--S3BucketName", "-B", dest="S3BucketName", required=True)
        parser.add_argument("--S3BucketPath", "-d", dest="S3BucketPath", default="")
        parser.add_argument("--verbose", action="store_true", dest="verbose", default=False)

        opts = parser.parse_args()
        self.verbose = opts.verbose
        if self.verbose:
            self.logger.setLevel(logging.DEBUG)

        self.webdav_hostname = opts.webDavHostname or ""
        self.webdav_path = opts.webDavPath
        self.webdav_filetype = opts.webDavFileType
        self.webdav_user = opts.webDAVuserID
        self.webdav_password = opts.webDAVpassword
        self.s3_bucket = opts.S3BucketName
        self.s3_prefix = opts.S3BucketPath or ""
        # normalize prefix to end with '/' or be empty
        if self.s3_prefix and not self.s3_prefix.endswith("/"):
            self.s3_prefix += "/"

        self.logger.debug("Options: webdav=%s path=%s filetype=%s s3=%s prefix=%s",
                          self.webdav_hostname, self.webdav_path, self.webdav_filetype,
                          self.s3_bucket, self.s3_prefix)
        return 0

    def _list_s3_objects(self) -> List[str]:
        """Return list of filenames already present in S3 under prefix (basename only)."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        existing = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                existing.append(Path(key).name)
        self.logger.debug("S3 existing files: %s", existing)
        return existing

    def _propfind_list(self) -> List[Dict]:
        """
        Perform a PROPFIND to list entries in the WebDAV path.
        Returns list of dictionaries with keys: name, href, contenttype, size, modified.
        """
        # build base URL
        base = self.webdav_hostname
        if not base.startswith("http://") and not base.startswith("https://"):
            base = "https://" + base
        # ensure path formatting
        webdav_url = base.rstrip("/") + "/" + self.webdav_path.lstrip("/")
        headers = {"Depth": "1"}
        body = """<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:resourcetype/>
    <D:getcontenttype/>
    <D:getcontentlength/>
    <D:getlastmodified/>
  </D:prop>
</D:propfind>"""
        auth = HTTPBasicAuth(self.webdav_user, self.webdav_password) if self.webdav_user else None
        resp = requests.request("PROPFIND", webdav_url, data=body, headers=headers, auth=auth, timeout=30)
        resp.raise_for_status()
        # parse XML response
        ns = {"D": "DAV:"}
        root = ET.fromstring(resp.content)
        items = []
        for response in root.findall("D:response", ns):
            href = response.find("D:href", ns)
            if href is None:
                continue
            href_text = href.text or ""
            prop = response.find("D:propstat/D:prop", ns)
            if prop is None:
                continue
            ct = prop.find("D:getcontenttype", ns)
            size = prop.find("D:getcontentlength", ns)
            lastmod = prop.find("D:getlastmodified", ns)
            name = Path(href_text).name
            # skip collection entries that represent the folder itself
            if not name:
                continue
            items.append({
                "name": name,
                "href": href_text,
                "contenttype": ct.text if ct is not None else "",
                "size": int(size.text) if size is not None and size.text and size.text.isdigit() else None,
                "modified": lastmod.text if lastmod is not None else None
            })
        self.logger.debug("WebDAV items: %s", items)
        return items

    def _download_file(self, href: str, dest_path: Path) -> None:
        """Download a file from WebDAV href to local dest_path."""
        # If href is relative, build full URL
        if href.startswith("/"):
            base = self.webdav_hostname
            if not base.startswith("http://") and not base.startswith("https://"):
                base = "https://" + base
            url = base.rstrip("/") + href
        else:
            url = href if href.startswith("http") else self.webdav_hostname.rstrip("/") + "/" + href.lstrip("/")
        auth = HTTPBasicAuth(self.webdav_user, self.webdav_password) if self.webdav_user else None
        with requests.get(url, stream=True, auth=auth, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        self.logger.info("Downloaded %s -> %s", url, dest_path)

    class _Progress:
        def __init__(self, logger: logging.Logger):
            self._seen = 0
            self._logger = logger

        def __call__(self, bytes_amount):
            # simple dot progress to stdout (keeps output compact)
            self._seen += bytes_amount
            sys.stdout.write(".")
            sys.stdout.flush()

    def _upload_to_s3(self, local_path: Path, s3_key: str) -> None:
        cb = self._Progress(self.logger)
        extra_args = {}
        # let boto3 infer content-type or set explicitly if desired
        self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key, Callback=cb, ExtraArgs=extra_args)
        sys.stdout.write("\n")
        self.logger.info("Uploaded to s3://%s/%s", self.s3_bucket, s3_key)

    def run(self) -> int:
        try:
            self.handle_options()
            self.logger.info("Starting backup run")
            existing = set(self._list_s3_objects())
            webdav_items = self._propfind_list()
            to_process = [i for i in webdav_items if self.webdav_filetype in (i.get("contenttype") or "")]
            if not to_process:
                self.logger.info("No WebDAV files matching type %s found.", self.webdav_filetype)
                return 0
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)
                for item in to_process:
                    name = item["name"]
                    if name in existing:
                        self.logger.info("%s already present in S3, skipping", name)
                        continue
                    dest = tmpdir_path / name
                    self._download_file(item["href"], dest)
                    s3_key = f"{self.s3_prefix}{name}"
                    self._upload_to_s3(dest, s3_key)
                    dest.unlink(missing_ok=True)
            self.logger.info("Backup run complete")
            return 0
        except requests.HTTPError as e:
            self.logger.error("HTTP error: %s", e)
            return 2
        except Exception as e:
            self.logger.exception("Unhandled error: %s", e)
            return 1


if __name__ == "__main__":
    rc = BackupSFCCInstance().run()
    sys.exit(rc)
