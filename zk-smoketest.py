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
parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="verbose output, include more detail")
parser.add_option("-q", "--quiet",
                  action="store_true", dest="quiet", default=False,
                  help="quiet output, basically just success/failure")

(options, args) = parser.parse_args()

zookeeper.set_log_stream(open("cli_log.txt","w"))

TIMEOUT = 10.0

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

class ZKClient(object):
    def __init__(self, servers, timeout):
        self.connected = False
        self.conn_cv = threading.Condition( )
        self.handle = -1

        self.conn_cv.acquire()
        if not options.quiet: print("Connecting to %s" % (servers))
        start = time.time()
        self.handle = zookeeper.init(servers, self.connection_watcher, 30000)
        self.conn_cv.wait(timeout)
        self.conn_cv.release()

        if not self.connected:
            raise SmokeError("Unable to connect to %s" % (servers))

        if not options.quiet:
            print("Connected in %d ms, handle is %d"
                  % (int((time.time() - start) * 1000), self.handle))

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        zookeeper.close(self.handle)
    
    def create(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        start = time.time()
        result = zookeeper.create(self.handle, path, data, acl, flags)
        if options.verbose:
            print("Node %s created in %d ms"
                  % (path, int((time.time() - start) * 1000)))
        return result

    def delete(self, path, version=-1):
        start = time.time()
        zookeeper.delete(self.handle, path, version)
        if options.verbose:
            print("Node %s deleted in %d ms"
                  % (path, int((time.time() - start) * 1000)))

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
        zookeeper.async(self.handle, path)

"""Callable watcher that counts the number of notifications"""
class CountingWatcher(object):
    def __init__(self):
        self.count = 0

    def waitForExpected(self, count, maxwait):
        """Wait up to maxwait seconds for the specified count,
        return the count whether or not maxwait reached.

        Arguments:
        - `count`: expected count
        - `maxwait`: max seconds to wait
        """
        waited = 0
        while (waited < maxwait):
            if self.count >= count:
                return self.count
            time.sleep(1.0);
            waited += 1
        return self.count

    def __call__(self, handle, typ, state, path):
        self.count += 1
        if options.verbose:
            print("handle %d got watch for %s in watcher, count %d" %
                  (handle, path, self.count))

"""Callable watcher that counts the number of notifications
and verifies that the paths are sequential"""
class SequentialCountingWatcher(CountingWatcher):
    def __init__(self, child_path):
        CountingWatcher.__init__(self)
        self.child_path = child_path

    def __call__(self, handle, typ, state, path):
        if not self.child_path(self.count) == path:
            raise SmokeError("handle %d invalid path order %s" % (handle, path))
        CountingWatcher.__call__(self, handle, typ, state, path)

class SmokeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


if __name__ == '__main__':
    servers = options.servers.split(",")

    # create all the sessions first to ensure that all servers are
    # at least available & quorum has been formed. otw this will 
    # fail right away (before we start creating nodes)
    sessions = []
    # create one session to each of the servers in the ensemble
    for i, server in enumerate(servers):
        sessions.append(ZKClient(server, TIMEOUT))

    # create on first server
    zk = ZKClient(servers[0], TIMEOUT)

    rootpath = "/zk-smoketest"

    if zk.exists(rootpath):
        if not options.quiet:
            print("Node %s already exists, attempting reuse" % (rootpath))
    else:
        zk.create(rootpath,
                  "smoketest root, delete after test done, created %s" %
                  (datetime.datetime.now().ctime()))

    # make sure previous cleaned up properly
    children = zk.get_children(rootpath)
    if len(children) > 0:
        raise SmokeError("there should not be children in %s"
                         "root data is: %s, count is %d"
                         % (rootpath, zk.get(rootpath), len(children)))

    zk.close()

    def child_path(i):
        return "%s/session_%d" % (rootpath, i)

    # create znodes, one znode per session (client), one session per server
    for i, server in enumerate(servers):
        # ensure this server is up to date with leader
        sessions[i].async()

        ## create child znode
        sessions[i].create(child_path(i), "", zookeeper.EPHEMERAL)

    watchers = []
    for i, server in enumerate(servers):
        # ensure this server is up to date with leader
        sessions[i].async()

        children = sessions[i].get_children(rootpath)
        if len(children) != len(sessions):
            raise SmokeError("server %s wrong number of children: %d" %
                             (server, len(children)))

        watchers.append(SequentialCountingWatcher(child_path))
        for child in children:
            sessions[i].get(rootpath + "/" + child, watchers[i])

    # trigger watches
    for i, server in enumerate(servers):
        sessions[i].delete(child_path(i))

    # check all watches fired
    for i, watcher in enumerate(watchers):
        # ensure this server is up to date with leader
        sessions[i].async()
        if watcher.waitForExpected(len(sessions), TIMEOUT) != len(sessions):
            raise SmokeError("server %s wrong number of watches: %d" %
                             (server, watcher.count))

    # close sessions
    for i, server in enumerate(servers):
        # ephemerals will be deleted automatically
        sessions[i].close()

    # cleanup root node
    zk = ZKClient(servers[-1], TIMEOUT)
    # ensure this server is up to date with leader (ephems)
    zk.async()
    zk.delete(rootpath)

    zk.close()

    print("Smoke test successful")
