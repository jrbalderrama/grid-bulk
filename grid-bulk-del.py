#!/usr/bin/env python

import os
import re
import sys
import subprocess
from threading import Thread
from Queue import Queue

MAX_THREADS_DELETE = 5

def execute(name, arguments):
    command = [name] 
    command += arguments
    process = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)    
    stdout, stderr = process.communicate()
    code = process.returncode
    if (code == 0):
        return stdout
    else:
        raise RuntimeError(stderr)


class CommandThread(Thread):

    def __init__(self, command, arguments):
        self.command = command
        self.arguments = arguments
        self.result = None
        Thread.__init__(self)

    def run(self):
        try:
            self.result = execute(self.command, self.arguments)
        except RuntimeError:            
            print "Error executing %s %s" % (self.command, self.arguments)
            os._exit(1)

class FileEraserThread(Thread):

    def __init__(self, path):
        self.path = path
        self.result = None
        Thread.__init__(self)

    def run(self):
        delete(self.path)

def delete(path):
    lfn = ["lfn:" + path]
    lcg_lg = CommandThread("lcg-lg", lfn)
    lcg_lr = CommandThread("lcg-lr", lfn)
    
    lcg_lg.start()
    lcg_lr.start()
    lcg_lg.join()
    lcg_lr.join()
    
    guid = lcg_lg.result.rstrip()
    surls = re.split('\n', lcg_lr.result.rstrip())
    lowlevel_delete(guid, surls, MAX_THREADS_DELETE)
    print "\t%s: deleted" % lfn[0]

def exists(path):
    exists = True
    arguments = [path]
    try:
        output = execute("lfc-ls", arguments)
    except RuntimeError:
        exists = False
    return exists
    
def islink(path):
    islink = True
    arguments = ["-l", path]
    try:
        long_list = execute("lfc-ls", arguments)
        if not(long_list.startswith('l')):
            islink = False
    finally:
        return islink

def isfile(path):
    if islink(path):
        return False
    isfile = True
    lfn = "lfn:" + path
    arguments = ["-l", "-d", lfn]
    try:
        long_list = execute("lcg-ls", arguments)
        if not(long_list.startswith('-')):
            isfile = False        
    except RuntimeError:
        isfile = False
    return isfile

def isdir(path):
    if islink(path):
        return False
    isdir = True
    lfn = "lfn:" + path
    arguments = ["-l", "-d", lfn]
    try:
        long_list= execute("lcg-ls", arguments)
        if not(long_list.startswith('d')):
            isdir = False
    except RuntimeError:
        isdir = False
    return isdir


def lst_contents(path):    
    contents = list()
    if isdir(path):
        arguments = [path]
        list_contents = execute("lfc-ls", arguments)
        if list_contents: # if dir is not empty
            nested_list = list_contents[:-1].split(os.linesep)        
            for item in nested_list:            
                subpath = os.path.join(path, item)
                subcontents = lst_contents(subpath)
                contents.extend(subcontents)
    elif isfile(path):
        contents.append(path)
    return contents

def lowlevel_delete(guid, surls, concurrent_process):

    finished = []
    
    def producer(queue, guid, surls):
        for surl in surls:
            arguments = [guid, surl]
            thread = CommandThread("lcg-uf", arguments)
            thread.start()
            queue.put(thread, True)

    def consumer(queue, surls):
        while len(finished) < surls:
            thread = queue.get(True)
            thread.join()
            finished.append(thread.result)

    queue = Queue(concurrent_process)
    producer = Thread(target=producer, args=(queue, guid, surls))
    consumer = Thread(target=consumer, args=(queue, len(surls)))    
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
    
def delete_directory(path, process):
    archives = lst_contents(path)
    finished = list()
    queue = Queue(process)

    def producer(queue, files):
        for archive in archives:
            thread = FileEraserThread(archive)
            thread.start()
            queue.put(thread, True)

    def consumer(queue, archives):
        while len(finished) < archives:
            thread = queue.get(True)
            thread.join()
            finished.append(thread.result)

    producer = Thread(target=producer, args=(queue, archives))
    consumer = Thread(target=consumer, args=(queue, len(archives)))    
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
            
    arguments = ["-r", path]
    execute("lfc-rm", arguments)
    
def main(argv):

    path = argv[0]
    if exists(path):
        if isfile(path):
            delete(path)
        elif isdir(path):
            message = "Do you really want to delete " + path + " and ALL its contents? (yes/no): "
            yesno = raw_input(message)
            if (yesno == 'yes'):
                delete_directory(path, 5)
    else:
        message = path + ": No such file or directory" + os.linesep
        sys.stderr.write(message)

if __name__=="__main__":
    main(sys.argv[1:])
    
