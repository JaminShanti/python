#!/usr/bin/env python
import sys
import socket
import paramiko
import ast
import multiprocessing
import re
import json
import argparse
import logging
from datetime import datetime

__author__ = 'jshanti'
__email__ = "devops@gmail.com"
__status__ = "Development"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('RecycleWebLogicServer')

def manage_container(command_option, sshusername, servername, containername, containerstatus, managescript, applicationserviceaccount):
    """
    Executes a remote command via SSH to manage a WebLogic container.
    """
    tmp_retcode = 0
    tmp_errormsgs = []

    logger.info("Action: '%s' | Container: '%s' | Machine: '%s' | Status: '%s' | User: '%s' | ServiceAccount: '%s'",
                command_option, containername, servername, containerstatus, sshusername, applicationserviceaccount)
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname=servername, username=sshusername)
        remote_command = "sudo -H -u %s %s -o '%s' -s '%s'" % (
            applicationserviceaccount, managescript, command_option, containername)
        
        logger.debug("Connecting to Server: '%s', running command: '%s'", servername, remote_command)
        
        stdin, stdout, stderr = ssh.exec_command(remote_command)
        return_stdout = stdout.read().decode('utf-8').strip()
        
        logger.debug("Response Message: '%s'", return_stdout)
        
        tmp_retcode += stdout.channel.recv_exit_status()
        
        if tmp_retcode > 0:
            if "java.lang.InterruptedException: sleep interrupted" in return_stdout:
                tmp_errormsgs.append("java.lang.InterruptedException: sleep interrupted")
        
        ssh.close()
        
        if command_option == "getstate":
            status_filter = re.compile(r"Current state of \".*\" : .*")
            results = re.findall(status_filter, return_stdout)
            if len(results) > 0:
                containerstatus = results[0].split(": ", 1)[-1]
        else:
            if tmp_retcode == 0:
                containerstatus = command_option.upper()

    except paramiko.AuthenticationException:
        logger.error("Authentication failed for %s@%s", sshusername, servername)
        tmp_retcode += 1
        tmp_errormsgs.append("Authentication failed")
    except paramiko.SSHException as e:
        logger.error("SSH error on %s: %s", servername, e)
        tmp_retcode += 1
        tmp_errormsgs.append(str(e))
    except socket.error as e:
        logger.error("Socket connection failed on %s: %s", servername, e)
        tmp_retcode += 1
        tmp_errormsgs.append(str(e))
    except Exception as e:
        logger.error("Unexpected error on %s: %s", servername, e)
        tmp_retcode += 1
        tmp_errormsgs.append(str(e))

    # Handle if script is stopped abruptly or returns specific exit codes
    if "Exiting Script. Return Code" in return_stdout:
        try:
            list_tmp_retcode = ast.literal_eval(return_stdout.split("Exiting Script. Return Code ", 1)[-1].strip())
            if isinstance(list_tmp_retcode, list) and len(list_tmp_retcode) > 0:
                tmp_retcode += list_tmp_retcode[0]
        except Exception as e:
            logger.warning("Failed to parse return code from stdout: %s", e)

    return_time = datetime.today()

    return tmp_retcode, tmp_errormsgs, servername, containername, containerstatus, return_time


class RecycleWebLogicServer(object):
    def __init__(self):
        self.hostname = socket.gethostname()
        self.targetInstanceList = []
        self.serverList = []
        self.serverListConfig = "/workspace/deploy/weblogicServerList.json"
        self.sshUserName = "userid"
        self.command_option = None
        self.statusFlag = False

    def parse_arguments(self):
        """
        Parses command line arguments using argparse.
        """
        parser = argparse.ArgumentParser(description="Manage WebLogic Server Instances")

        parser.add_argument("-s", "--serverList", dest="serverList", required=True, nargs='+', 
                            help="List of servers (comma-separated or space-separated). Example: -s server1,server2")
        parser.add_argument("-l", "--serverListConfig", dest="serverListConfig", 
                            default="/workspace/deploy/weblogicServerList.json",
                            help="Configuration File providing server container information")
        parser.add_argument("-u", "--sshUserName", dest="sshUserName", default="userid",
                            help="SSH Username")
        
        # Action group (mutually exclusive)
        action_group = parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument("--resume", dest="action", action="store_const", const="resume", help="Resume instances")
        action_group.add_argument("--suspend", dest="action", action="store_const", const="suspend", help="Suspend instances")
        action_group.add_argument("--restart", dest="action", action="store_const", const="restart", help="Restart instances")
        action_group.add_argument("--start", dest="action", action="store_const", const="start", help="Start instances")
        action_group.add_argument("--stop", dest="action", action="store_const", const="stop", help="Stop instances")
        action_group.add_argument("--status", dest="action", action="store_const", const="getstate", help="Check status")

        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Enable verbose logging")

        args = parser.parse_args()

        # Handle server list parsing
        if args.serverList:
            if "," in args.serverList[0]:
                self.serverList = args.serverList[0].split(",")
            else:
                self.serverList = args.serverList

        self.serverListConfig = args.serverListConfig
        self.sshUserName = args.sshUserName
        self.command_option = args.action
        
        if args.verbose:
            logger.setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")

        if self.command_option == "getstate":
            self.statusFlag = True

        logger.info("Arguments: Servers=%s, Action='%s'", self.serverList, self.command_option)
        return 0, []

    def load_configuration(self):
        """
        Loads the WebLogic server configuration from a JSON file.
        """
        retcode = 0
        errormsgs = []
        try:
            logger.info("Loading configuration file: %s", self.serverListConfig)
            with open(self.serverListConfig, 'r') as data_file:
                instancelist = json.load(data_file)
            
            logger.debug("Full instance list loaded: %s", instancelist)
            
            for instance in instancelist:
                if instance.get("machineName") in self.serverList:
                    logger.info("Discovered Managed Container: '%s' on Machine: '%s'", 
                                instance.get("containerName"), instance.get("machineName"))
                    self.targetInstanceList.append(instance)
            
            if not self.targetInstanceList:
                logger.warning("No matching containers found for the provided server list.")
                
        except IOError as e:
            logger.error("Failed to read configuration file: %s", e)
            retcode = 1
            errormsgs.append(str(e))
        except ValueError as e:
            logger.error("Invalid JSON in configuration file: %s", e)
            retcode = 1
            errormsgs.append(str(e))
        except Exception as e:
            logger.error("Unexpected error loading configuration: %s", e)
            retcode = 1
            errormsgs.append(str(e))

        return retcode, errormsgs

    def execute_tasks(self, action_override=None):
        """
        Executes the management tasks in parallel.
        """
        retcode = 0
        errormsgs = []
        tasks = []
        
        command_to_run = action_override if action_override else self.command_option
        
        jobmaxcount = multiprocessing.cpu_count()
        logger.info("Execution Pool: Server='%s', CPUs='%s', MaxJobs='%s'", self.hostname, jobmaxcount, jobmaxcount)
        
        pool = multiprocessing.Pool(jobmaxcount)
        
        # Build task list
        for instance in self.targetInstanceList:
            logger.debug("Queueing task for instance: %s", instance.get("containerName"))
            tasks.append((command_to_run, self.sshUserName, instance["machineName"], instance["containerName"],
                          instance.get("containerStatus", "UNKNOWN"),
                          instance["manageScript"], instance["applicationServiceAccount"]))

        # Run tasks
        try:
            results = [pool.apply_async(manage_container, t) for t in tasks]
            pool.close()
            pool.join()
            
            # Process results
            for result in results:
                (tmp_retcode, tmpErrorMsgs, servername, containername, containerstatus, return_time) = result.get()
                
                log_level = logging.INFO if tmp_retcode == 0 else logging.ERROR
                logger.log(log_level, "Result: Time='%s' | Code='%d' | Server='%s' | Container='%s' | Status='%s' | Errors='%s'",
                           return_time, tmp_retcode, servername, containername, containerstatus, tmpErrorMsgs)
                
                retcode += tmp_retcode
                errormsgs.extend(tmpErrorMsgs)
                
                # Update internal state
                for instance in self.targetInstanceList:
                    if instance["machineName"] == servername and instance["containerName"] == containername:
                        instance["containerStatus"] = containerstatus
                        
        except Exception as e:
            logger.error("Error during parallel execution: %s", e)
            retcode += 1
            errormsgs.append(str(e))
        finally:
            pool.terminate()

        return retcode, errormsgs

    def run(self):
        """
        Main execution flow.
        """
        retcode, errormsgs = self.parse_arguments()
        if retcode != 0:
            sys.exit(retcode)

        retcode, msgs = self.load_configuration()
        errormsgs.extend(msgs)
        
        if retcode != 0:
            logger.error("Configuration load failed. Exiting.")
            sys.exit(retcode)

        if self.statusFlag:
            # Just check status
            rc, msgs = self.execute_tasks(action_override="getstate")
            retcode += rc
            errormsgs.extend(msgs)
        else:
            # Pre-check status
            logger.info("--- Pre-Check Status ---")
            rc, msgs = self.execute_tasks(action_override="getstate")
            retcode += rc
            errormsgs.extend(msgs)
            
            if retcode == 0:
                # Perform Action
                logger.info("--- Performing Action: %s ---", self.command_option)
                rc, msgs = self.execute_tasks()
                retcode += rc
                errormsgs.extend(msgs)
                
                # Post-check status
                logger.info("--- Post-Check Status ---")
                rc, msgs = self.execute_tasks(action_override="getstate")
                retcode += rc
                errormsgs.extend(msgs)

        logger.info("--- Execution Summary ---")
        if retcode == 0:
            logger.info("All operations completed successfully.")
        else:
            logger.error("Operations completed with errors.")
            for msg in errormsgs:
                logger.error("Error: %s", msg)
            
            for instance in self.targetInstanceList:
                if instance.get("containerStatus") != "RUNNING":
                     logger.warning("Instance Issue: Machine='%s' | Container='%s' | Status='%s'", 
                                    instance["machineName"], instance["containerName"], instance.get("containerStatus"))

        sys.exit(retcode)

if __name__ == '__main__':
    app = RecycleWebLogicServer()
    app.run()
