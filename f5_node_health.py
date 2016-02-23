__author__ = 'jshanti'
##############################################################
#
#  Created On:  08/29/2014
#  Author:  Jamin Shanti
#  Purpose: audit pool nodes for status - alert on non-healthy
#  Using Marks' handleOptions
###############################################################
import bigsuds
import getpass
from datetime import datetime, timedelta
from optparse import OptionParser
from optparse import OptionGroup
import os
import sys
import logging

logging.basicConfig(filename=__file__ + ".log",level=logging.WARN,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def main():

    print (datetime.today())
    print("Running " + os.path.basename(__file__))
    parser = OptionParser()

    group = OptionGroup(parser, "If absent will use default")
    group.add_option("--f5_hostname", dest="f5hostname", default="server1", help="ex. server1, server2, server3 ")
    #currently not checking unique tcl files
    #group.add_option("--irule", dest="irule", default=None, help="ex. services4_acme_com_irule_external , services4_acme_com_irule_external")
    group.add_option("--site_name", dest="site_name", default="Patch", help="ex. Patch BETA COMM")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()

    f5hostname = options.f5hostname
    f5username =  getpass.getuser()
    #irule = options.irule
    site_name = options.site_name
    # not asking for f5 password at commandline.
    f5password = getpass.getpass("Enter password for " + f5hostname + "\\" + f5username + ": ")

    #big IP check
    try:
        b= bigsuds.BIGIP(hostname=f5hostname, username=f5username, password=f5password,debug=True,cachedir=None)
    except bigsuds.ConnectionError :
        print "Authentication failed for some reason"
        sys.exit()
    print 'Connecting to ' + f5hostname + '...'
    print b.LocalLB.Pool.get_version()
    if "server2" in f5hostname:
        b.Management.Partition.set_active_partition('egd-tier2')
    if "server1" in f5hostname:
        b.Management.Partition.set_active_partition('partitionName')
    if "server3" in f5hostname:
        b.Management.Partition.set_active_partition('partitionName1')
    pool_list = filter(lambda x:site_name in x, b.LocalLB.Pool.get_list())
    for pool in pool_list:
        print pool
        try:
            node = b.LocalLB.PoolMember.get_session_status([pool])[0]
        except bigsuds.ServerError:
            b.Management.Partition.set_active_partition('Common')
            node = b.LocalLB.PoolMember.get_session_status([pool])[0]
            b.Management.Partition.set_active_partition('egd-tier2')
        for item in range(0,len(node) ):
            if "SESSION_STATUS_ENABLED" != node[item]['session_status']:
                print node[item]
        try:
            node = b.LocalLB.PoolMember.get_monitor_status([pool])[0]
        except bigsuds.ServerError:
            b.Management.Partition.set_active_partition('Common')
            node = b.LocalLB.PoolMember.get_monitor_status([pool])[0]
            b.Management.Partition.set_active_partition('egd-tier2')
        for item in range(0,len(node) ):
            if "MONITOR_STATUS_UP" != node[item]['monitor_status']:
                print node[item]


if __name__ == '__main__':
    main()