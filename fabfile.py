# -*- coding: iso-8859-1 -*-

from fabric.api import env, task, run
import sys
import os
import subprocess


# ---------- Remote or local definition ----------
env.local_command = False

@task(alias = "lcmd")
def local_command():
    env.local_command = True


# ---------- Start clients ----------
# Default values
workingDirectory = "demo"
clientExecutablePath = "../client.py"
configFilePath = "config.demo.xml"
numClients = 1
 
@task(alias = "snc")    
def start_n_clients(n = numClients, dir = workingDirectory, client = clientExecutablePath, config = configFilePath):
    n = int(n)
    if (env.local_command):
        os.chdir(dir)
        for i in range(n):
            subprocess.Popen(["python", client, config, "-v", "off"])
    else:
        # Can't use 'with cd(dir)' here because Fabric hangs. The change of directory, the loop 
        # and the execution of the clients are all done on the remote machine through bash
        executeClient = "python {0} {1} -v off".format(client, config)
        loop = "for i in {{1..{0}}}; do {1} & done >& /dev/null".format(n, executeClient)
        changeDir = "cd {0}".format(dir)
        command = "bash -c '{0} && {1}'".format(changeDir, loop)
        run(command, pty=False)
                