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

import zookeeper, time, datetime
from optparse import OptionParser

import zkclient
from zkclient import ZKClient, TIMEOUT, SequentialCountingWatcher, zookeeper

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers", dest="servers",
                  default="localhost:2181", help="comma separated list of host:port")
parser.add_option("", "--root_znode", dest="root_znode",
                  default="/zk-latencies", help="root for the test (will be created)")
parser.add_option("", "--znode_size", dest="znode_size",
                  default=25, help="data size when creating/setting znodes (default 100)")
parser.add_option("", "--test_latencies",
                  action="store_true", dest="test_latencies", default=False,
                  help="specifying this option runs create/delete/get/set/watch operations (default off)")
parser.add_option("", "--znode_count", dest="znode_count", default=10000,
                  help="the number of znodes to operate on in each performance section")

parser.add_option("", "--force",
                  action="store_true", dest="force", default=False,
                  help="force the test to run, even if root_znode exists - WARNING! don't run this on a real znode or you'll lose it!!!")

parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="verbose output, include more detail")
parser.add_option("-q", "--quiet",
                  action="store_true", dest="quiet", default=False,
                  help="quiet output, basically just success/failure")

(options, args) = parser.parse_args()
options.znode_count = int(options.znode_count)
options.znode_size = int(options.znode_size)

zkclient.options = options

zookeeper.set_log_stream(open("cli_log.txt","w"))

class SmokeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def start():
    global _start
    _start = time.time()

def end(msg, count=options.znode_count):
    global _start
    elapms = (time.time() - _start) * 1000
    print("%s in %d ms (%f ms/op %f/sec)"
          % (msg, int(elapms), elapms/count, count/(elapms/1000.0)))


if __name__ == '__main__':
    servers = options.servers.split(",")

    # create all the sessions first to ensure that all servers are
    # at least available & quorum has been formed. otw this will 
    # fail right away (before we start creating nodes)
    sessions = []
    # create one session to each of the servers in the ensemble
    for i, server in enumerate(servers):
        sessions.append(ZKClient(server))

    # ensure root_znode doesn't exist
    if sessions[0].exists(options.root_znode):
        # unless forced by user
        if not options.force:
            raise SmokeError("Node %s already exists!" % (options.root_znode))

        children = sessions[i].get_children(options.root_znode)
        for child in children:
            sessions[0].delete("%s/%s" % (options.root_znode, child))
    else:
        sessions[0].create(options.root_znode,
                           "smoketest root, delete after test done, created %s" %
                           (datetime.datetime.now().ctime()))


    def child_path(i):
        return "%s/session_%d" % (options.root_znode, i)

    for i, server in enumerate(servers):
        print("Testing latencies on server %s" % (server))

        # create znode_count znodes (perm)
        start()
        for j in xrange(options.znode_count):
            sessions[i].create(child_path(j), "")
        end("created %d permanent znodes" % (options.znode_count))

        # delete znode_count znodes
        start()
        for j in xrange(options.znode_count):
            sessions[i].delete(child_path(j))
        end("deleted %d permanent znodes" % (options.znode_count))

        # create znode_count znodes (ephemeral)
        start()
        for j in xrange(options.znode_count):
            sessions[i].create(child_path(j), "", zookeeper.EPHEMERAL)
        end("created %d ephemeral znodes" % (options.znode_count))

        # delete znode_count znodes
        start()
        for j in xrange(options.znode_count):
            sessions[i].delete(child_path(j))
        end("deleted %d ephemeral znodes" % (options.znode_count))

    sessions[0].delete(options.root_znode)

    # close sessions
    for i, server in enumerate(servers):
        sessions[i].close()

    print("Latency test complete")
