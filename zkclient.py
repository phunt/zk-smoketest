import zookeeper, time, datetime, threading

TIMEOUT = 10.0

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

class ZKClient(object):
    def __init__(self, servers, timeout=TIMEOUT):
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

