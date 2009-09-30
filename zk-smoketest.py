#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import zookeeper, time, datetime, threading
from optparse import OptionParser

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers", dest="servers",
                  default="localhost:2181", help="comma separated list of host:port")

TIMEOUT = 10.0

(options, args) = parser.parse_args()

zookeeper.set_log_stream(open("cli_log.txt","w"))

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

class ZKClient(object):
    def __init__(self, servers, timeout):
        self.connected = False
        self.conn_cv = threading.Condition( )
        self.handle = -1

        self.conn_cv.acquire()
        print("Connecting to " + servers)
        self.handle = zookeeper.init(servers, self.connection_watcher, 30000)
        self.conn_cv.wait(timeout)
        self.conn_cv.release()

        print "Connected, handle is ", self.handle

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        zookeeper.close(self.handle)
    
    def create(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        return zookeeper.create(self.handle, path, data, acl, flags)

    def delete(self, path, version=-1):
        zookeeper.delete(self.handle, path, version)

    def get(self, path, watcher=None):
        return zookeeper.get(self.handle, path, watcher)

    def exists(self, path, watcher=None):
        return zookeeper.exists(self.handle, path, watcher)

    def set(self, path, data="", version=-1):
        zookeeper.set(self.handle, path, data, version)

    def set2(self, path, data="", version=-1):
        return zookeeper.set2(self.handle, path, data, version)


    def get_children(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)

    def async(self, path = "/"):
        print("ASYNC")
        zookeeper.async(self.handle, path)
        #time.sleep(1)

class SmokeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

if __name__ == '__main__':
    servers = options.servers.split(",")

    # create on first server
    zk = ZKClient(servers[0], TIMEOUT)

    rootpath = "/zk-smoketest"

    if zk.exists(rootpath):
        print("Node %s already exists, attempting reuse" % (rootpath))
    else:
        zk.create(rootpath,
                  "smoketest root, delete after test done, created %s" %
                  (datetime.datetime.now().ctime()))

    # make sure previous cleaned up properly
    children = zk.get_children(rootpath)
    if len(children) > 0:
        raise SmokeError("there should be not children in %s"
                         "root data is: %s, count is %d"
                         % (rootpath, zk.get(rootpath), len(children)))

    zk.close()

    def child_path(i):
        return "%s/session_%d" % (rootpath, i)

    sessions = []
    # create znodes, one znode per client, one client per server
    for i, server in enumerate(servers):
        sessions.append(ZKClient(server, TIMEOUT))

        ## create child znode
        sessions[i].create(child_path(i), "", zookeeper.EPHEMERAL)

    class Watcher(object):
        def __init__(self):
            self.count = 0

        def __call__(self, handle, typ, state, path):
            self.count += 1

    watchers = []
    for i, server in enumerate(servers):
        # ensure this server is up to date with leader
        sessions[i].async()

        children = sessions[i].get_children(rootpath)
        if len(children) != len(sessions):
            raise SmokeError("server %s wrong number of children: %d" %
                             (server, len(children)))

        watchers.append(Watcher())
        sessions[i].get(child_path(i), watchers[i])

    # trigger watches
    for i, server in enumerate(servers):
        sessions[i].delete(child_path(i))

    # check all watches fired
    for i, watcher in enumerate(watchers):
        # ensure this server is up to date with leader
        sessions[i].async()
        if watcher.count != len(sessions):
            raise SmokeError("server %s wrong number of watches: %d" %
                             (server, watcher.count))

    # close sessions
    for i, server in enumerate(servers):
        # ephemerals will be deleted automatically
        sessions[i].close()

    # cleanup root node
    zk = ZKClient(servers[-1], TIMEOUT)
    # ensure this server is up to date with leader (ephems)
    sessions[i].async()
    zk.delete(rootpath)

    zk.close()
