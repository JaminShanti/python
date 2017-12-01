##!/usr/bin/env python
from datetime import datetime
import os
import socket
from argparse import ArgumentParser
import easywebdav
import boto
import boto.s3
from boto.s3.key import Key
import tempfile
import shutil
import sys

__author__ = 'jshanti'
__email__ = "devops@gmail.com"
__status__ = "Development"


class BackupSFCCInstance(object):
    def __init__(self):
        self.hostname = socket.gethostname()
        self.S3CurrentBackupList = []
        self.instanceName = []
        self.verbose = False
        return

    def percent_cb(self, complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    def sizeof_fmt(self, num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def upload_file_to_S3_bucket(self, uploadFile):
        print 'Uploading %s to Amazon S3 bucket %s' % \
              (uploadFile, self.S3BucketName)
        k = Key(self.s3Bucket)
        k.key = self.S3BucketPath + uploadFile
        k.set_contents_from_filename(self.dirpath + '/' + uploadFile,
                                     cb=self.percent_cb, num_cb=10)
        return retcode, errormsgs

    def handle_options(self):
        retcode = 0
        errormsgs = []
        parser = ArgumentParser()

        group = parser.add_argument_group('group')

        group.add_argument("--webDavHostname", '-H', dest="webDavHostname", action="store",
                           help="webDavLocation, default=None")
        group.add_argument("--webDavPath", '-p', dest="webDavPath", action="store",
                           default='on/demandware.servlet/webdav/Sites/Impex/src/instance/',
                           help="webDavPath, default=None")
        group.add_argument("--webDavFileType", '-T', dest="webDavFileType", action="store", default='application/zip',
                           help="webDavFileType, default=application/zip")
        group.add_argument("--webDAVuserID", '-U', dest="webDAVuserID", action="store", default="backup",
                           help="userID for webDav Mount, default=backup")
        group.add_argument("--webDAVpassword", '-P', dest="webDAVpassword", action="store",
                           help="Password for webDAVuserID, default=None")
        group.add_argument("--S3BucketName", '-B', dest="S3BucketName", action="store",
                           help="S3 BucketName, default=None")
        group.add_argument("--S3BucketPath", '-d', dest="S3BucketPath", action="store",
                           help="Destination path for backups, default=webDavHostname/backups/")
        group.add_argument("--verbose", dest="verbose", action="store_true", default=False,
                           help="Be verbose, default=False")

        options = parser.parse_args()

        self.verbose = options.verbose
        self.logprint("Argument provided: verbose '%s'" % (self.verbose), "INFO")
        self.webDavHostname = options.webDavHostname
        self.logprint("Argument provided: webDavHostname '%s'" % (self.webDavHostname), "INFO")
        self.webDavPath = options.webDavPath
        self.logprint("Argument provided: webDavPath '%s'" % (self.webDavPath), "INFO")
        self.webDavFileType = options.webDavFileType
        self.logprint("Argument provided: webDavFileType '%s'" % (self.webDavFileType), "INFO")
        self.webDAVuserID = options.webDAVuserID
        self.logprint("Argument provided: webDAVuserID '%s'" % (self.webDAVuserID), "INFO")
        self.webDAVpassword = options.webDAVpassword
        self.logprint("Argument provided: webDAVpassword '.....'", "INFO")
        self.S3BucketName = options.S3BucketName
        self.logprint("Argument provided: S3Bucket '%s'" % (self.S3BucketName), "INFO")
        self.S3BucketPath = options.S3BucketPath
        self.logprint("Argument provided: S3BucketPath '%s'" % (self.S3BucketPath), "INFO")
        self.logprint("%s%d, %s%s" % ("Retcode=", retcode, "ErrorMessages=", errormsgs), "INFO")
        return retcode, errormsgs

    def connectS3_and_getCurrentBackupList(self):
        self.logprint("Connecting to AWS S3...", "INFO")
        self.s3 = boto.connect_s3()
        self.logprint("Connecting to S3Location: '%s:%s'" % (self.S3BucketName, self.S3BucketPath), 'INFO')
        self.s3Bucket = self.s3.get_bucket(self.S3BucketName)
        self.s3CurrentBackupObject = self.s3Bucket.list(prefix=self.S3BucketPath, delimiter='/')
        for backedupFile in self.s3CurrentBackupObject:
            backupFilenameOnly = backedupFile.name.rsplit('/', 1)[1]
            self.S3CurrentBackupList.append(backupFilenameOnly)
        self.logprint("The Current File Count is : '%s'" % (len(self.S3CurrentBackupList)), 'INFO')
        self.logprint("The S3CurrentBackupList is '%s'" % (self.S3CurrentBackupList), 'DEBUG')
        return retcode, errormsgs

    def connectWebDAV_and_Backupfiles_toS3(self):
        self.logprint("Connecting to WebDAV Instance '%s'" % self.webDavHostname, "INFO")
        self.webdav = easywebdav.connect(self.webDavHostname,
                                         username=self.webDAVuserID,
                                         password=self.webDAVpassword,
                                         protocol='https')
        self.logprint("Inspecting File Path:  '%s'" % self.webDavPath, "INFO")
        self.returnFileList = self.webdav.ls(self.webDavPath)
        self.logprint("Locating FileType:  '%s'" % self.webDavFileType, "INFO")
        self.returnFileListSelected = [x for x in self.returnFileList if self.webDavFileType in x.contenttype]
        self.dirpath = tempfile.mkdtemp()
        self.logprint("Creating Directory '%s'" % (self.dirpath), 'DEBUG')
        for webdavfile in self.returnFileListSelected:
            webdavfileName = webdavfile.name.rsplit('/', 1)[1]
            self.logprint("Discovered: '%s' size '%s' Modified_Time '%s' '" % (
                webdavfileName, self.sizeof_fmt(webdavfile.size), webdavfile.mtime), 'INFO')
            if webdavfileName in self.S3CurrentBackupList:
                self.logprint("Is Present in S3 as '%s:%s%s'" % (self.S3BucketName, self.S3BucketPath, webdavfileName),
                              'INFO')
            else:
                self.logprint("'%s' Backup is New!!" % (webdavfileName), 'INFO')
                self.logprint("saving to %s/%s" % (self.dirpath, webdavfileName), 'INFO')
                self.webdav.download(self.webDavPath + webdavfileName,
                                     self.dirpath + "/" + webdavfileName)
                self.logprint("Uploading as '%s:%s%s'" % (self.S3BucketName, self.S3BucketPath, webdavfileName), 'INFO')
                self.upload_file_to_S3_bucket(webdavfileName)
                self.logprint("Removing to %s/%s" % (self.dirpath, webdavfileName), 'INFO')
                os.remove(self.dirpath + '/' + webdavfileName)
        self.logprint("Removing Directory '%s'" % (self.dirpath), 'DEBUG')
        shutil.rmtree(self.dirpath)

        return retcode, errormsgs

    def logprint(self, msg, level):
        if self.verbose:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + logSpace + level + logSpace + msg)
        elif level != 'DEBUG':
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + logSpace + level + logSpace + msg)


if __name__ == '__main__':
    logSpace = '  '
    print(
    datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + logSpace + 'INFO' + logSpace + os.path.basename(__file__))
    retcode = 0
    errormsgs = []
    currentDirectory = os.getcwd()
    backupSFCCInstance = BackupSFCCInstance()

    tmpRetcode, tmpErrorMsgs = backupSFCCInstance.handle_options()
    tmpRetcode, tmpErrorMsgs = backupSFCCInstance.connectS3_and_getCurrentBackupList()
    tmpRetcode, tmpErrorMsgs = backupSFCCInstance.connectWebDAV_and_Backupfiles_toS3()

    retcode += tmpRetcode
    errormsgs += tmpErrorMsgs
