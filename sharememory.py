import ctypes
import ctypes.wintypes
#from .error import handle_nonzero_success
import win32event
import hashlib
import cPickle as pickle
import struct
import mmap
import time
import uuid
import base64


FILE_MAP_ALL_ACCESS = 0xF001F
CreateFileMapping = ctypes.windll.kernel32.CreateFileMappingW
CreateFileMapping.argtypes = [
    ctypes.wintypes.HANDLE,
    ctypes.c_void_p,
    ctypes.wintypes.DWORD,
    ctypes.wintypes.DWORD,
    ctypes.wintypes.DWORD,
    ctypes.wintypes.LPWSTR,
]
CreateFileMapping.restype = ctypes.wintypes.HANDLE

MapViewOfFile = ctypes.windll.kernel32.MapViewOfFile
MapViewOfFile.restype = ctypes.wintypes.HANDLE


def UUID():
#    return uuid.uuid1().hex.upper()
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).strip("=")

class MemoryMap(object):
    """
    A memory map object which can have security attributes overrideden.
    """
    def __init__(self, name, length, security_attributes=None):
#        print '__init__', length
        self.name = name
        self.length = length
        self.security_attributes = security_attributes
        self.pos = 0

    def __enter__(self):
        print '__enter__'
        p_SA = (
            ctypes.byref(self.security_attributes)
            if self.security_attributes else None
        )
        INVALID_HANDLE_VALUE = -1
        PAGE_READWRITE = 0x4
        FILE_MAP_WRITE = 0x2
        filemap = ctypes.windll.kernel32.CreateFileMappingW(
            INVALID_HANDLE_VALUE, p_SA, PAGE_READWRITE, 0, self.length,
            unicode(self.name))
        #handle_nonzero_success(filemap)
        if filemap == INVALID_HANDLE_VALUE:
            raise Exception("Failed to create file mapping")
        self.filemap = filemap
        self.view = MapViewOfFile(filemap, FILE_MAP_ALL_ACCESS, 0, 0, self.length)
#        print 'view', self.view
        return self

    def open(self):
        p_SA = (
            ctypes.byref(self.security_attributes)
            if self.security_attributes else None
        )
        INVALID_HANDLE_VALUE = -1
        PAGE_READWRITE = 0x4
        FILE_MAP_WRITE = 0x2
        filemap = ctypes.windll.kernel32.CreateFileMappingW(
            INVALID_HANDLE_VALUE, p_SA, PAGE_READWRITE, 0, self.length,
            unicode(self.name))
        #handle_nonzero_success(filemap)
        if filemap == INVALID_HANDLE_VALUE:
            raise Exception("Failed to create file mapping")
        self.filemap = filemap
        self.view = MapViewOfFile(filemap, FILE_MAP_ALL_ACCESS, 0, 0, self.length)
#        print 'view', self.view

    def seek(self, pos):
        self.pos = pos

    def write(self, msg):
#        print len(msg)
        ctypes.cdll.msvcrt.memcpy(self.view, msg, len(msg))
        self.pos += len(msg)

    def read(self, n, pos=0):
        """
        Read n bytes from mapped view.
        """
        out = ctypes.create_string_buffer(n)
        ctypes.cdll.msvcrt.memcpy(out, (self.view + pos ), n)
        #self.pos += n
        return out.raw

#    def __exit__(self, exc_type, exc_val, tb):
#        print '__exit__'
#        ctypes.windll.kernel32.UnmapViewOfFile(self.view)
#        ctypes.windll.kernel32.CloseHandle(self.filemap)

    def close(self):
#        print '__del__'
        ctypes.windll.kernel32.UnmapViewOfFile(self.view)
        ctypes.windll.kernel32.CloseHandle(self.filemap)


class Semaphore(object):
    def __init__(self, name):
        self.name = name
        self.lock = win32event.CreateSemaphore(None,1,1,self.name)

    def request(self):
#        print 'Semaphore request'
        win32event.WaitForSingleObject(self.lock, win32event.INFINITE)

    def release(self):
#        print 'Semaphore release'
        win32event.ReleaseSemaphore(self.lock, 1)

    def __del__(self):
        self.lock.close()

class shareobject(object):
    def __init__(self, name):
        key = hashlib.sha1(name).hexdigest()
        self.sharememory_name_size = 'qipc_sharedmemory_size_{}{}'.format(name, key)
        self.sharememory_name = 'qipc_sharedmemory_{}{}'.format(name, key)
        self.lock_name = 'qipc_systemsem_{}{}'.format(name, key)
        self.global_lock_name = 'qipc_global_systemsem_{}{}'.format(name, key)

        self.sharememory_size = MemoryMap(self.sharememory_name_size, 8)
        self.sharememory_size.open()
#        self.sharememory_size.write(struct.pack('Q', 0))

        self.sharememory_object = MemoryMap(self.sharememory_name, 100*1024*1024)
        self.sharememory_object.open()

        self._lock = Semaphore(self.lock_name)
        self._global_lock = Semaphore(self.global_lock_name)

    def lock(self):
        self._global_lock.request()

    def unlock(self):
        self._global_lock.release()

    def write(self, o):
        s = pickle.dumps(o, 2)
#        print 'write size', len(s)
        self._lock.request()
        self.sharememory_object.close()
        self.sharememory_size.write(struct.pack('Q', len(s)))
        self.sharememory_object = MemoryMap(self.sharememory_name, len(s))
        self.sharememory_object.open()
        self.sharememory_object.write(s)
        self._lock.release()

    def read(self):
        self._lock.request()
        self.sharememory_object.close()
        s = self.sharememory_size.read(8)
#        print [s]
        (size,) = struct.unpack('Q', s)
#        print 'read size', size
        if size <= 0:
            self._lock.release()
            return None
        self.sharememory_object = MemoryMap(self.sharememory_name, size)
        self.sharememory_object.open()
        s = self.sharememory_object.read(size)
        self._lock.release()
        try:
            return pickle.loads(s)
        except EOFError:
            return None

    def __del__(self):
        del(self.sharememory_object)
        del(self.sharememory_size)
        del(self._lock)
        del(self._global_lock)

class shareobject_mmap(object):
    def __init__(self, name):
        key = hashlib.sha1(name).hexdigest()
        self.sharememory_name_size = 'qipc_sharedmemory_size_{}{}'.format(name, key)
        self.sharememory_name = 'qipc_sharedmemory_{}{}'.format(name, key)
        self.lock_name = 'qipc_systemsem_{}{}'.format(name, key)

        self.sharememory_size = mmap.mmap(-1, 8, self.sharememory_name_size)
        self.sharememory_size.write(struct.pack('Q', 0))

        self.sharememory_object = mmap.mmap(-1, 8, self.sharememory_name, mmap.ACCESS_WRITE|mmap.ACCESS_READ)

        self.lock = Semaphore(self.lock_name)

    def write(self, o):
        s = pickle.dumps(o, 2)
        self.lock.request()
        self.sharememory_size.seek(0)
        self.sharememory_size.write(struct.pack('Q', len(s)))
#        self.sharememory_object.close()
#        del(self.sharememory_object)
        self.sharememory_object = mmap.mmap(-1, len(s), self.sharememory_name)
#        self.sharememory_object.resize(len(s)+100)
        self.sharememory_object.seek(0)
        self.sharememory_object.write(s)
        self.lock.release()

    def read(self):
        self.lock.request()
        self.sharememory_size.seek(0)
        s = self.sharememory_size.read(8)
#        print [s]
        (size,) = struct.unpack('Q', s)
#        print 'size', size
#        self.sharememory_object.close()
#        del(self.sharememory_object)
        self.sharememory_object = mmap.mmap(-1, size, self.sharememory_name)
#        self.sharememory_object.resize(size+100)
        self.sharememory_object.seek(0)
        s = self.sharememory_object.read(size)
        self.lock.release()
        return pickle.loads(s)

    def __del__(self):
        self.sharememory_object.close()
        self.sharememory_size.close()
        del(self.lock)

class ShareDict(dict):
    def __init__(self, name):
        self._shareobject = shareobject(name)
    
    def _load(self):
#        dict.clear(self)
        t = self._shareobject.read()
#        print '_load', t
        if t:
            for k in t:
                dict.__setitem__(self, k, t[k])

    def __setitem__(self, key, item):
#        print '__setitem__', key, item
        self._shareobject.lock()
        self._load()
        dict.__setitem__(self, key, item)
#        print dict(self)
        self._shareobject.write(dict(self))
        self._shareobject.unlock()
    
    def __getitem__(self, key):
        self._load()
        return dict.__getitem__(self, key)

    def clear(self):
        dict.clear(self)
        self._shareobject.write({})
        
    def __str__(self):
        self._load()
        return dict.__str__(self)

    def __repr__(self):
        self._load()
        return dict.__repr__(self)

class global_variables(object):
    def __init__(self):
        self._mem = ShareDict('mem')
        self._mem['global'] = {}

    def __getattr__(self, n):
        temp = object.__getattribute__(self, '_mem')['global']
        if n in temp:
            return temp[n]
        else:
            return None

    def __setattr__(self, n, v):
        if n == '_mem':
            object.__setattr__(self, n, v)
        else:
            temp = object.__getattribute__(self, '_mem')['global']
            temp[n] = v
            self._mem['global'] = temp
        

class Commands_linkage(object):
    def __init__(self):
        self._sd = ShareDict('cl')
        self._sd['commands'] = []
        self._uuid = ''
#        print '_sd', self._sd
        
    def write(self, cmd):
        cmds = self._sd['commands']
        cmds = cmds[-9:]
#        cmds.append([uuid.uuid1().hex, cmd])
        cmds.append([UUID(), cmd])
        self._sd['commands'] = cmds

    def read(self):
        cmds = self._sd['commands']
#        print 'read', cmds
        uuids = [u for u, c in cmds]
        if self._uuid in uuids:
            n = uuids.index(self._uuid)
            r = [c for t, c in cmds][n+1:]
        else:
            r = [c for t, c in cmds]
        self._uuid = uuids[-1]
        return r



if __name__ == '__main__':
    g1 = global_variables()
    g2 = global_variables()
    g1.user =  'test_user'
    print g2.user
    g2.user = 'default_user'
    print g1.user
    for i in xrange(100):
        g1.user = 'user{}'.format(i)
        print g2.user
    import sys
    sys.exit()
    cl1 = Commands_linkage()
    cl2 = Commands_linkage()
    for i in xrange(100):
        cl1.write(3*i)
        cl1.write(3*i+1)
        cl1.write(3*i+1.5)
        cl1.write(3*i+2)
#        print cl2._sd
        print cl2.read()
    import sys
    sys.exit()
    sd1 = ShareDict('tt')
    sd2 = ShareDict('tt')
    sd1['123'] = 123
    sd2['commands'] = []
    print sd1
    print sd2
    print '#' * 50
    sd2['234'] = 234
    print '#' * 50
    print [sd1]
    print sd2
    import sys
    sys.exit()
    class test():
        def __init__(self, name):
            self.name = name
            self.a = 'a'
            self.b = 'b'

        def __repr__(self):
            return '< class test with name {} >'.format(self.name)

    m1 = shareobject('tt')
    m2 = shareobject('tt')
    t = []
    for i in xrange(1000):
        t.append( test('t{}'.format(i)))
    m1.write(t)
    import time
    t = time.time()
    c = m2.read()
    print time.time() - t
    t = {}
    for i in xrange(100):
        t[str(i)] = test('t{}'.format(i))
    m1.write(t)
    t = time.time()
    m2.read()
    print time.time() - t
    t = []
    mm = shareobject('test1')
    for i in xrange(100):
        t.append( test('t{}'.format(i)))
        t = t[-4:]
        print 'write'
        mm.write(t)
        print 'sleep'
        time.sleep(2)
