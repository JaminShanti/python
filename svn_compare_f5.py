__author__ = 'jshanti'
##############################################################
#
#  Created On:  08/25/2014
#  Author:  Jamin Shanti
#  Purpose: Compare SVN with Live F5 tcl
#  Using Marks' handleOptions
###############################################################
import bigsuds
import getpass
from datetime import datetime, timedelta
from optparse import OptionParser
from optparse import OptionGroup
import os
import subprocess
import difflib
import logging

logging.basicConfig(filename=os.path.basename(__file__)+'.log',level=logging.WARN)
logging.getLogger('suds.client').setLevel(logging.INFO)




def main():

    print (datetime.today())
    print("Running " + os.path.basename(__file__))
    parser = OptionParser()

    group = OptionGroup(parser, "If absent will use default")
    group.add_option("--f5_hostname", dest="f5hostname", default="server1", help="ex. server1, servername, server3 ")
    #currently not checking unique tcl files
    #group.add_option("--irule", dest="irule", default=None, help="ex. services4_acme_com_irule_external , services4_acme_com_irule_external")
    group.add_option("--svn_location", dest="svn_location", default="https://svn.acme.com/svn/online/deployment/branches/2014.02/acme/config/services-patch/f5/", help="ex. https://svn.acme.com/svn/online/deployment/trunk/acme/config/dgnorth/beta/f5")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()

    f5hostname = options.f5hostname
    f5username =  getpass.getuser()
    #irule = options.irule
    svn_location = options.svn_location
    # not asking for f5 password at commandline.
    f5password = getpass.getpass("Enter password for " + f5hostname + "\\" + f5username + ": ")

    #svn check first
    print "svn location : " + svn_location
    p = subprocess.Popen("svn ls " + svn_location, stdout=subprocess.PIPE, shell=True)
    p.wait()
    print "tcl files are:"
    tcl_name = [line.rstrip('\n\r') for line in p.stdout]
    # check for non tcl files
    for item in tcl_name:
        if ".tcl" not in item: tcl_name.remove(item)
    print tcl_name
    tcl_list = []
    for item in tcl_name:
        record = {}
        p = subprocess.Popen("svn cat " + svn_location + tcl_name[tcl_name.index(item)], stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        record['rule_name'] = tcl_name[tcl_name.index(item)]
        record['rule_definition'] = output
        tcl_list.append(record)

    #big IP check
    b= bigsuds.BIGIP(hostname=f5hostname, username=f5username, password=f5password,debug=True,cachedir=None)
    print 'Connecting to ' + f5hostname + '...'
    print b.LocalLB.Pool.get_version()
    # I was going to put this in the options but what's the point.  All our pools and rules are on this partition.
    if "servername" in f5hostname:
        b.Management.Partition.set_active_partition('partitionName')
    #print b.LocalLB.Rule.get_list()

    irule_files = []

    for item in tcl_name:
        try:
            irule_files.append(b.LocalLB.Rule.query_rule([tcl_name[tcl_name.index(item)].rstrip('.tcl')])[0])
        except:
            record = {}
            record['rule_name'] = tcl_name[tcl_name.index(item)]
            record['rule_definition'] = "Not Found"
            irule_files.append(record)

    #file compare
    for item in tcl_name:
        if irule_files[tcl_name.index(item)]['rule_definition'] == "Not Found":
            print "irule: " + irule_files[tcl_name.index(item)]['rule_name'] + " not present on F5: "+f5hostname
        else:
            if tcl_list[tcl_name.index(item)]['rule_definition'] == irule_files[tcl_name.index(item)]['rule_definition']:
                print "irule: " + irule_files[tcl_name.index(item)]['rule_name'] + " matches SVN: " + tcl_list[tcl_name.index(item)]['rule_name']
            else:
                print "irule: " + irule_files[tcl_name.index(item)]['rule_name'] + " does not match SVN: " + tcl_list[tcl_name.index(item)]['rule_name']
                diff=difflib.context_diff(irule_files[tcl_name.index(item)]['rule_definition'],tcl_list[tcl_name.index(item)]['rule_definition'])
                print ''.join(diff)



if __name__ == '__main__':
    main()