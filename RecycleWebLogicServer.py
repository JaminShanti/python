##!/usr/bin/env python
import sys
import socket
import paramiko
import ast
import multiprocessing
import re
import json
from argparse import ArgumentParser
from datetime import datetime

__author__ = 'jshanti'
__email__ = "devops@gmail.com"
__status__ = "Development"


def manage_container(command_option, sshusername, servername, containername, containerstatus, managescript,applicationserviceaccount):
    tmp_retcode = 0
    tmp_errormsgs = []

    print "Completing Action: '%s' Container: '%s' on Machine: '%s' Current_Status: '%s' SSH_User: '%s' Application_Service_Account: '%s'" % (
        command_option, containername, servername, containerstatus, sshusername, applicationserviceaccount)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname=servername, username=sshusername)
        remote_command = "sudo -H -u %s %s -o '%s' -s '%s'" % (
            applicationserviceaccount, managescript, command_option, containername)
        vprint("\t\t Connecting to Server: '%s', running command : '%s'" % (servername, remote_command))
        stdin, stdout, stderr = ssh.exec_command(remote_command)
        return_stdout = stdout.read()[:-1]
        vprint("\t\t Response Message: '%s'" % return_stdout)
        tmp_retcode += stdout.channel.recv_exit_status()
        if tmp_retcode > 0:
            if "java.lang.InterruptedException: sleep interrupted" in return_stdout:
                tmp_errormsgs.append("java.lang.InterruptedException: sleep interrupted")
        ssh.close()
        if command_option == "getstate":
            status_filter = re.compile("Current state of \".*\" : .*")
            results = re.findall(status_filter, return_stdout)
            if len(results) > 0:
                containerstatus = results[0].split(": ", 1)[-1]
        else:
            if tmp_retcode == 0:
                containerstatus = command_option.upper()


    except paramiko.AuthenticationException:
        print "Authentication failed for some reason"
    except paramiko.SSHException, e:
        print "Password is invalid:", e
    except socket.error, e:
        print "Socket connection failed on %s:" % servername, e
    # handle if script is stopped abruptly
    if "Exiting Script. Return Code" not in return_stdout:
        tmp_retcode += 1
    else:
        list_tmp_retcode = ast.literal_eval(return_stdout.split("Exiting Script. Return Code ", 1)[-1].strip())
        tmp_retcode += list_tmp_retcode[0]

    return_time = datetime.today()


    return tmp_retcode, tmp_errormsgs, servername, containername, containerstatus, return_time


def vprint(msg):
    if recycleWeblogicServer.verbose:
        print(msg)


class RecycleWebLogicServer(object):
    def __init__(self):
        self.hostname = socket.gethostname()
        self.targetInstanceList = []
        self.instanceList = []
        self.verbose = False
        return

    def handle_options(self):
        retcode = 0
        errormsgs = []
        parser = ArgumentParser()

        group = parser.add_argument_group('group')

        group.add_argument("--serverList", "-s", dest="serverList", required=False, nargs='+', type=str,
                           help="ServerList is required.  Example -s server1,server2,server3")
        group.add_argument("--serverListConfig", "-l", dest="serverListConfig", action="store",
                           default="/workspace/deploy/weblogicServerList.json",
                           help="Configuration File proving server container information, default=/workspace/deploy/weblogicServerList.json")
        group.add_argument("--sshUserName", "-u", dest="sshUserName", action="store", default="userid",
                           help="sshUsername, default=userid")
        group.add_argument("--resume", dest="resumeFlag", action="store_true", default=False,
                           help="Weather To resume weblogic instances, default=False")
        group.add_argument("--suspend", dest="suspendFlag", action="store_true", default=False,
                           help="Weather To suspend weblogic instances, default=False")
        group.add_argument("--restart", dest="restartFlag", action="store_true", default=False,
                           help="Weather To restart weblogic instances, default=False")
        group.add_argument("--start", dest="startFlag", action="store_true", default=False,
                           help="Weather To start weblogic instances, default=False")
        group.add_argument("--stop", dest="stopFlag", action="store_true", default=False,
                           help="Weather To stop weblogic instances, default=False")
        group.add_argument("--status", dest="statusFlag", action="store_true", default=False,
                           help="Check Status of weblogic instances, default=False")
        group.add_argument("--verbose", dest="verbose", action="store_true", default=False,
                           help="Be verbose, default=False")

        options = parser.parse_args()


        if "," in options.serverList[0]:
            self.serverList = options.serverList[0].split(",")
        else:
            self.serverList = options.serverList

        if options.serverListConfig:
            self.serverListConfig = options.serverListConfig

        self.sshUserName = options.sshUserName
        self.verbose = options.verbose

        anyflag = 0
        if options.resumeFlag:
            anyflag += 1
        if options.suspendFlag:
            anyflag += 1
        if options.restartFlag:
            anyflag += 1
        if options.startFlag:
            anyflag += 1
        if options.stopFlag:
            anyflag += 1
        if options.statusFlag:
            anyflag += 1

        if anyflag > 0:
            self.suspendFlag = options.suspendFlag
            self.resumeFlag = options.resumeFlag
            self.restartFlag = options.restartFlag
            self.startFlag = options.startFlag
            self.stopFlag = options.stopFlag
            self.statusFlag = options.statusFlag
        else:
            self.restartFlag = False
            self.startFlag = False
            self.stopFlag = False
            self.statusFlag = False
            self.resumeFlag = False
            self.suspendFlag = False

        if self.stopFlag:
            self.command_option = "stop"
        elif self.suspendFlag:
            self.command_option = "suspend"
        elif self.resumeFlag:
            self.command_option = "resume"
        elif self.startFlag:
            self.command_option = "start"
        elif self.restartFlag:
            self.command_option = "restart"
        elif self.statusFlag:
            self.command_option = "getstate"

        else:
            self.command_option = None

        self.vprint(
            "\t\t Arguments provided are Servernames: '%s', command_option set to '%s'" % (
                self.serverList, self.command_option))
        self.vprint("\t\t %s%d, %s%s" % ("Retcode=", retcode, "ErrorMessages=", errormsgs))
        return retcode, errormsgs

    def check_weblogic_configuration(self):
        retcode = 0
        errormsgs = []
        try:
            self.vprint(
                "\t\t Importing configuration file : weblogicServerList.json")
            with open(self.serverListConfig) as data_file:
                instancelist = json.load(data_file)
        except Exception as e:
            print e
        self.vprint("\t\t Complete List of Containers found:'%s'" % instancelist)
        for instance in instancelist:
            if instance["machineName"] in self.serverList:
                print "Discovered Managed Container: '%s' on Machine: '%s'" % (
                    instance["containerName"], instance["machineName"])
                self.targetInstanceList.append(instance)

        self.vprint("\t\t Total targetInstnaceList: '%s'" % self.targetInstanceList)

        self.vprint("\t\t %s%d, %s%s" % ("Retcode=", retcode, "ErrorMessages=", errormsgs))
        return retcode, errormsgs

    def check_container_status(self):
        retcode = 0
        errormsgs = []
        tasks = []
        jobmaxcount = multiprocessing.cpu_count()
        print "Running on Server: '%s' CPUCount: '%s' Maximum asyncJobs: '%s'" % (
            self.hostname, jobmaxcount, jobmaxcount)
        pool = multiprocessing.Pool(jobmaxcount)
        # Build task list
        for instance in self.targetInstanceList:
            self.vprint("\t\t Attempting instance: '%s'" % instance)
            tasks.append(("getstate", self.sshUserName, instance["machineName"], instance["containerName"], instance["containerStatus"],
                 instance["manageScript"],instance["applicationServiceAccount"]))

        # Run tasks
        results = [pool.apply_async(manage_container, t) for t in tasks]
        # Process results
        for result in results:
            (tmp_retcode, tmpErrorMsgs, servername, containername, containerstatus, return_time) = result.get()
            print("Result: Timestamp: '%s' retcode '%d' errormsg '%s' Server: '%s' Container: '%s' Status: '%s'" % (
                return_time, tmp_retcode, tmpErrorMsgs, servername, containername, containerstatus))
            retcode += tmp_retcode
            errormsgs += tmpErrorMsgs
            # update targetInstanceList
            for instance in self.targetInstanceList:
                if instance["machineName"] == servername and instance["containerName"] == containername:
                    instance["containerStatus"] = containerstatus
        pool.close()
        pool.join()

        self.vprint("\t\t %s%d, %s%s" % ("Retcode=", retcode,"ErrorMessages=",errormsgs))
        return retcode, errormsgs

    def manage_target_servers(self):
        retcode = 0
        errormsgs = []
        tasks = []

        jobmaxcount = multiprocessing.cpu_count()
        print "Running on Server: '%s' CPUCount: '%s' Maximum asyncJobs: '%s'" % (
            self.hostname, jobmaxcount, jobmaxcount)
        pool = multiprocessing.Pool(jobmaxcount)
        # Build task list
        for instance in self.targetInstanceList:
            self.vprint("\t\t Attempting instance: '%s'" % instance)
            tasks.append((self.command_option, self.sshUserName, instance["machineName"], instance["containerName"],
                          instance["containerStatus"],
                          instance["manageScript"], instance["applicationServiceAccount"]))

        # Run tasks
        results = [pool.apply_async(manage_container, t) for t in tasks]
        # Process results
        for result in results:
            (tmp_retcode, tmpErrorMsgs, servername, containername, containerstatus, return_time) = result.get()
            print("Result: Timestamp: '%s' retcode '%d' errormsg '%s' Server: '%s' Container: '%s' Status: '%s'" % (
                return_time, tmp_retcode, tmpErrorMsgs, servername, containername, containerstatus))
            retcode += tmp_retcode
            errormsgs += tmpErrorMsgs
            # update targetInstanceList
            for instance in self.targetInstanceList:
                if instance["machineName"] == servername and instance["containerName"] == containername:
                    instance["containerStatus"] = containerstatus
        pool.close()
        pool.join()

        self.vprint("\t\t %s%d, %s%s" % ("Retcode=", retcode, "ErrorMessages=", errormsgs))
        return retcode, errormsgs

        # Calling container Status
        tmp_retcode, tmpErrorMsgs = self.check_container_status()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs
        self.vprint("\t\t %s%d, %s%s" % ("Retcode=", retcode, "ErrorMessages=", errormsgs))
        return retcode, errormsgs

    def vprint(self, msg):
        if self.verbose:
            print(msg)


if __name__ == '__main__':
    print("recycleWeblogicServer")
    print (datetime.today())
    retcode = 0
    errormsgs = []
    recycleWeblogicServer = RecycleWebLogicServer()

    tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.handle_options()
    retcode += tmpRetcode
    errormsgs += tmpErrorMsgs

    if retcode == 0:
        tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.check_weblogic_configuration()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs
    if not recycleWeblogicServer.statusFlag and retcode == 0:
        tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.check_container_status()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs
        tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.manage_target_servers()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs
        tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.check_container_status()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs
    elif recycleWeblogicServer.statusFlag and retcode == 0:
        tmpRetcode, tmpErrorMsgs = recycleWeblogicServer.check_container_status()
        retcode += tmpRetcode
        errormsgs += tmpErrorMsgs

    print (datetime.today())
    if retcode == 0:
        print ("No problems found.")
    else:
        print ("One or more problems found!")
        for errormsg in errormsgs:
            print "\t - %s" % errormsg
            print "\t Retcode=%d" % retcode
        for instance in recycleWeblogicServer.targetInstanceList:
            if instance["containerStatus"] <> "RUNNING":
                print "Machine: '%s' Container: '%s' Current_Status: '%s'" % (instance["machineName"],instance["containerName"],instance["containerStatus"])

    sys.exit(retcode)
