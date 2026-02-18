import sys
import os
import argparse
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger('WebLogicStatus')

def setup_logger(verbose=False):
    """
    Sets up the logger with the specified verbosity.
    """
    level = logging.DEBUG if verbose else logging.INFO
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    logger.setLevel(level)

def connect_weblogic_domain(username, password, admin_url):
    """
    Connects to the WebLogic Admin Server.
    """
    logger.info("Connecting to '%s' as '%s'", admin_url, username)
    try:
        # WLST connect function (available in WLST environment)
        connect(username, password, admin_url)
        logger.info("Successfully connected to WebLogic Domain.")
    except Exception as e:
        logger.error("Failed to connect to WebLogic Domain: %s", e)
        sys.exit(1)

def get_server_state(server_name):
    """
    Retrieves the runtime state of a specific server.
    Switches to domainRuntime tree and back.
    """
    state = "undefined"
    try:
        # Capture current tree to restore later
        # In WLST, currentTree() returns the current MBean tree object.
        previous_tree = currentTree()
        
        # Switch to domainRuntime to get runtime status
        domainRuntime()
        
        # Navigate to ServerLifeCycleRuntimes
        cd('ServerLifeCycleRuntimes/' + server_name)
        state = cmo.getState()
        
    except Exception as e:
        logger.debug("Could not get state for server '%s'. It might be down or unreachable. Error: %s", server_name, e)
        state = "undefined"
    finally:
        # Restore original tree
        try:
            previous_tree()
        except Exception:
            # Fallback if previous_tree() is not callable, try switching to serverConfig
            try:
                serverConfig()
            except:
                pass
            
    return state

def get_target_servers(app_name):
    """
    Navigates the AppDeployments tree to find target servers and their statuses.
    """
    instance_list = []
    deployment_path = "AppDeployments/%s" % app_name
    
    logger.info("Navigating to deployment path: %s", deployment_path)
    
    try:
        cd(deployment_path)
    except Exception as e:
        logger.error("Application '%s' not found or path invalid: %s", app_name, e)
        return instance_list

    try:
        # List children of the application node
        child_nodes = ls(returnMap='true', returnType='c')
        
        if 'Targets' in child_nodes:
            logger.debug("Found 'Targets' node.")
            cd('Targets')
            
            # List clusters or servers targeted
            targets = ls(returnMap='true', returnType='c')
            
            for target_name in targets:
                logger.debug("Processing target: %s", target_name)
                cd(target_name)
                
                sub_nodes = ls(returnMap='true', returnType='c')
                
                if 'Servers' in sub_nodes:
                    # It is a Cluster
                    logger.debug("Target '%s' is a Cluster.", target_name)
                    cd('Servers')
                    servers = ls(returnMap='true', returnType='c')
                    
                    for server_name in servers:
                        logger.debug("Processing server in cluster: %s", server_name)
                        cd(server_name)
                        
                        machine_name = "undefined"
                        if 'Machine' in ls(returnMap='true', returnType='c'):
                            cd('Machine')
                            machines = ls(returnMap='true', returnType='c')
                            if machines:
                                machine_name = machines[0]
                            cd('..') # Back to server
                        
                        state = get_server_state(server_name)
                        instance_list.append([app_name, target_name, server_name, machine_name, state])
                        
                        cd('..') # Back to Servers
                    
                    cd('..') # Back to Cluster
                    
                elif 'Machine' in sub_nodes:
                    # It is a standalone Server
                    server_name = target_name
                    logger.debug("Target '%s' is a Standalone Server.", server_name)
                    
                    machine_name = "undefined"
                    cd('Machine')
                    machines = ls(returnMap='true', returnType='c')
                    if machines:
                        machine_name = machines[0]
                    cd('..') # Back to server
                    
                    state = get_server_state(server_name)
                    instance_list.append([app_name, "noCluster", server_name, machine_name, state])
                
                cd('..') # Back to Targets
            
            cd('..') # Back to AppDeployments/AppName
            
        # Navigate back to AppDeployments
        cd('..') 
        
    except Exception as e:
        logger.error("Error traversing targets: %s", e)
        
    return instance_list

def parse_arguments():
    """
    Parses command line arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Get WebLogic Server Status for an Application")
    
    parser.add_argument("-u", "--username", dest="username", help="Console User Name", 
                        default=os.getenv('WL_ADMIN_USERNAME'))
    parser.add_argument("-p", "--password", dest="password", help="Console Password", 
                        default=os.getenv('WL_ADMIN_PASSWORD'))
    parser.add_argument("-a", "--adminURL", dest="adminURL", help="Admin URL (e.g. t3://localhost:7001)", 
                        default=os.getenv('WL_ADMIN_T3_URL'))
    parser.add_argument("-i", "--application", dest="application", help="Application Name", 
                        default="generic-application")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    return parser.parse_args()

def main():
    args = parse_arguments()
    
    setup_logger(args.verbose)
    
    if not args.username or not args.password or not args.adminURL:
        logger.error("Missing required credentials/URL. Set environment variables (WL_ADMIN_USERNAME, WL_ADMIN_PASSWORD, WL_ADMIN_T3_URL) or provide arguments.")
        sys.exit(1)
        
    logger.info("Starting WebLogic Server Status Check")
    
    connect_weblogic_domain(args.username, args.password, args.adminURL)
    
    server_array = get_target_servers(args.application)
    
    logger.info("Status Check Complete.")
    logger.info("Output:")
    for row in server_array:
        logger.info(row)

if __name__ == "__main__":
    main()
