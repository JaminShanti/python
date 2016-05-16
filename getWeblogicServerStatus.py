import sys
import getopt
import os
from datetime import datetime


def usage():
    print "Usage:"
    print "getWeblogicServerStatus.py [--vsName virtual_Server_Name] [--listenAddress listen_Address] " \
          "[--port listen_Port] [--adminURL admin_URL] [--application applicaitonName]"


def connect_weblogic_domain():
    print "Connecting to '%s' as '%s" % (adminURL,username)
    connect(username, password, adminURL)


def getTargetServers(childNodeName):
    instanceList = []
    cd(childNodeName)
    childNodeListing = ls(returnMap='true', returnType='c')
    if childNodeListing.size() > 0:
        for childNode in childNodeListing:
            if childNode == 'Targets':
                print("\t\t change directory to: '%s'" % childNode)
                cd(childNode)
                # added for cluster support
                clusterNames = ls(returnMap='true', returnType='c')
                if clusterNames.size() > 0:
                    for clusterNm in clusterNames:
                        print("\t\t change directory to: '%s'" % clusterNm)
                        cd(clusterNm)
                        clusterNodeListing = ls(returnMap='true', returnType='c')
                        for clusterNode in clusterNodeListing:
                            if clusterNode == 'Servers':
                                print("\t\t change directory to: '%s'" % clusterNode)
                                cd(clusterNode)
                                serverNames = ls(returnMap='true', returnType='c')
                                if serverNames.size() > 0:
                                    for srvrNm in serverNames:
                                        print("\t\t change directory to: '%s'" % srvrNm)
                                        cd(srvrNm)
                                        print("\t\t change directory to: '%s'" % 'Machine')
                                        cd('Machine')
                                        machineNm = ls(returnMap='true', returnType='c')
                                        if not machineNm:
                                            machineNm += ["undefined"]
                                        # way to much work to get server status
                                        myTree = currentTree()
                                        domainRuntime()
                                        cd('ServerLifeCycleRuntimes/' + srvrNm)
                                        stateNm = cmo.getState()
                                        myTree()
                                        instanceList.append(
                                            ["generic-application", clusterNm, srvrNm, machineNm[0], stateNm])
                                        cd('..')
                                        cd('..')
                            if clusterNode == 'Machine':
                                # clusternode is actually a Container/srvrNm
                                srvrNm = clusterNm
                                print("\t\t ServerName: '%s'" % srvrNm)
                                print("\t\t change directory to: '%s'" % 'Machine')
                                cd('Machine')
                                machineNm = ls(returnMap='true', returnType='c')
                                if not machineNm:
                                    machineNm += ["undefined"]
                                # way to much work to get server status
                                myTree = currentTree()
                                domainRuntime()
                                cd('ServerLifeCycleRuntimes/' + srvrNm)
                                stateNm = cmo.getState()
                                try:
                                    stateNm
                                except IndexError:
                                    stateNm = "undefined"
                                myTree()
                                instanceList.append(
                                    ["generic-application", "noCluster", srvrNm, machineNm[0], stateNm])
                                cd('..')
                        cd('..')
                        cd('..')
                cd('..')
                # navigate to parent of Targets node
        cd('..')
    # navigate back to parent attribute
    cd('..')
    return instanceList


# ===== Sourcing content from Environemnt settings ===================
try:
    username = os.getenv('WL_ADMIN_USERNAME')
    password = os.getenv('WL_ADMIN_PASSWORD')
    adminURL = os.getenv('WL_ADMIN_T3_URL')
    applicationName= "generic-application"
except:
    pass
# ====== Main program ===============================
try:
    opts, args = getopt.getopt(sys.argv[1:], "u:p:a:i:",
                               ["consoleUserName=", "consolePassword=", "adminURL=","application="])
except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

for opt, arg in opts:
    if opt in ("-u", "--consoleUserName"):
        username = arg
    elif opt in ("-p", "--consolePassword"):
        password = arg
    elif opt in ("-a", "--adminURL"):
        adminURL = arg
    elif opt in ("-i", "--application"):
        applicationName = arg


# verify environemnt variables are defined.d
try:
    username
    password
    adminURL
except NameError:
    print "Error: Environment Variables not Set..."
    sys.exit(1)
else:
    print "Environment Variables set..."
print("getWeblogicServerStatus")
print (datetime.today())
connect_weblogic_domain()
serverArray = getTargetServers("AppDeployments/%s" % applicationName)
print (datetime.today())
print("start output here:")
print(serverArray)
