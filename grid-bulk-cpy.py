#!/usr/bin/env python

import os
import re
import sys
import subprocess
from datetime import datetime
from threading import Thread
from Queue import Queue

DEFAULT_THREADS = 2
VO = os.getenv("LCG_GFAL_VO", default="biomed")
_SE_ENV_NAME = "VO_" + str.upper(VO) + "_DEFAULT_SE"
DEFAULT_SE = os.getenv(_SE_ENV_NAME, default="se.grid.rug.nl")

def execute(name, arguments):
    command = [name] 
    command += arguments
    process = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)    
    stdout, stderr = process.communicate()
    code = process.returncode
    if (code == 0):
        # print stdout
        return code
    else:
        raise RuntimeError(stderr)

class CommandThread(Thread):

    def __init__(self, commands, arguments, comments):
        self.commands = commands
        self.arguments = arguments
        self.comments = comments
        self.result = None

        Thread.__init__(self)

    def run(self):
        for command, parameters, comment in zip(self.commands, self.arguments, self.comments):
            try:
                # print "Executing %s %s" % (command, parameters)
                self.result = execute(command, parameters)
                if (command == "lcg-cr"):
                    self.result = "DONE=" + parameters[5]
                    print "\t%s: copied" % parameters[5]
                if (command == "lcg-ls"):
                    self.result = "SKIP=" + parameters[0]
                    print "\t%s: already there, copy skipped!" % parameters[0]
                    break
            except RuntimeError, stderr:
                if (command == "lcg-cr"):
                    self.result = "FAIL=" + parameters[5] + ";" + parameters[9]
                    print "\tError while copying %s" %  parameters[5]
                if (command == "lfc-mkdir"):
                    self.result = "FLMD=" + comment 
                    print "\tError while creating directory for %s" % comment.split(";")[0]
                    break
                # print "\tError executing %s %s" % (command, parameters)
                # print stderr  
                # os._exit(1)

def concurrent_copy(paths, storage, process):
    finished = list()
    queue = Queue(process)
    dati = datetime.now()
    pwd = os.getcwd()    
    name = os.path.join(pwd, "bulk-cpy_" + dati.strftime("%y%m%d-%H%M%S") + ".log")
    log = open(name, "w") 
    log.write("## log <" + str(dati) + ">")
    log.write(os.linesep)

    def producer(queue, paths, storage):
        for source, destination in paths.iteritems():
            commands = list()
            arguments = list()
            comments = list()
            parent = os.path.dirname(destination)
            destination = "lfn:" + destination
            ### (1) check if the file is already registered
            ### using a hack by running lcg-ls
            commands.append("lcg-ls")
            parameters = [destination]
            arguments.append(parameters)
            comments.append(None)
            ### (2) make destination directory            
            commands.append("lfc-mkdir")
            parameters = ["-p", parent]
            arguments.append(parameters)
            comments.append(destination + ";file:" + source)
            ### (3) perform copy to the storage element
            commands.append("lcg-cr")
            ### streams for copy: each 10Mb=1 stream  
            streams =  str(1 + os.path.getsize(source)/(10*1024**2) % 4)
            source = "file:" + source
            parameters = ["--vo", VO, "-d", storage, "-l", destination, "-n", streams, "--checksum", source]
            arguments.append(parameters)
            comments.append(None)
            ### start one thread for bunch of executions
            thread = CommandThread(commands, arguments, comments)
            thread.start()
            queue.put(thread, True)

    def consumer(queue, paths):
        while len(finished) < paths:
            thread = queue.get(True)
            thread.join()
            result = thread.result
            finished.append(result)
            log.write(result + os.linesep)
            # force to write contents to disk
            log.flush()
            os.fsync(log.fileno())

    producer = Thread(target=producer, args=(queue, paths, storage))
    consumer = Thread(target=consumer, args=(queue, len(paths)))    
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
    log.write ("## eof <" + str(datetime.now()) + ">")
    log.write(os.linesep)
    log.close()
    return finished

def list_with_depth(path, depth=0):
    dictionary = dict()        
    normalized = os.path.normpath(path)
    if os.path.isfile(normalized):
        dictionary[normalized] = depth    
    else:        
        archives = os.listdir(path)
        for archive in archives:
            archive_path = os.path.join(path, archive)
            subdict = list_with_depth(archive_path, depth + 1)
            dictionary.update(subdict)
    return dictionary

def get_base_path(path, depth, root):
    ### root should be and abstract path
    if (depth == 0):
        ### the path is a file
        return ""
    elif (depth == 1):
        ### the path is a directory containing files
        return root[root.rfind("/") + 1:] 
    else:
        ### the path includes nested contents
        path = re.sub(root, "", path)
        return path[1:path.rfind("/")] 

def main(argv):
    source = os.path.abspath(argv[0])
    grid = argv[1]
    storage = DEFAULT_SE 
    if (len(argv) > 2):
        storage = argv[2]
    threads = DEFAULT_THREADS 
    if (len(argv) > 3):
         threads = argv[3]
    archives = list_with_depth(source)
    paths = dict()
    for archive, depth in archives.iteritems():        
        path = get_base_path(archive, depth, source) 
        filename = os.path.basename(archive)
        destination = os.path.join(grid, path, filename)
        paths[archive] = destination
    report = concurrent_copy(paths, storage, threads)

if __name__ == "__main__":
    main(sys.argv[1:])
