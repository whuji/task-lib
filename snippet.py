#!/usr/bin/env python3
# coding: utf-8
# vi: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys
import signal
import threading
import tasksManagerThreaded

mustExit = threading.Event()

def signal_handler(signal, frame):
    mustExit.set()

def taskHandler(task):
    print("CALLBACK: ",task)
    if task.type == "ip_scan":
        task.update() # Transitioning to "In progress"
        task.result.append({"hostname": "asa.dev.nadratec.com", "ip": "10.0.0.6", "os_name": "Linux", "os_version": "4.9.0-3-amd64", "credentials_id": 1 })
        task.complete() # Transitioning to "Completed"
    else:
        pass

#def taskViewer(task):
    #print("TASK VIEWER: ",task)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    #mytask = tasks.Task(create=(2,"bla", '{ "my": "world"}'))

    t = tasksManagerThreaded.TaskManagerThreaded(db_config=("127.0.0.1", "test", "postgres"))
    print(t.listTasks())
    t.subscribeChannelCallback("task", taskHandler, sql_filter_tasks="type ~ '.*' AND status = 'created'")
    #t.subscribeChannelCallback("task", taskViewer, sql_filter_tasks="type !~ '.*_scan'", read_only=True)
    t.start()

    mustExit.wait()
    t.stop()
    t.join()

    return

if __name__ == '__main__':
    sys.exit(main())
