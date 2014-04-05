from . import *

from aque.utils import *
from aque import utils


class TestBytesFormatting(TestCase):

    def test_format_bytes(self):
        self.assertEqual(format_bytes(0), '0B')
        self.assertEqual(format_bytes(1), '1B')
        self.assertEqual(format_bytes(1023), '1023B')
        self.assertEqual(format_bytes(1024), '1kB')
        self.assertEqual(format_bytes(1024**2), '1MB')
        self.assertEqual(format_bytes(1024**3), '1GB')
        self.assertEqual(format_bytes(1024**4), '1TB')
        self.assertEqual(format_bytes(1024**5), '1PB')
        self.assertEqual(format_bytes(1024**6), '1EB')
        self.assertEqual(format_bytes(1024**7), '1ZB')
        self.assertEqual(format_bytes(1024**8), '1YB')
        self.assertEqual(format_bytes(1024**9), '1024YB')

    def test_round_trip(self):

        for x in 0, 1, 1023, 1024, 1024**2, 1024**3:
            y = format_bytes(x)
            z = parse_bytes(y)
            self.assertEqual(x, z)
        

class TestMountParsing(TestCase):

    def test_darwin_mounts(self):
        mounts = list(utils._parse_darwin_mounts('''
/dev/disk1 on / (hfs, local, journaled)
devfs on /dev (devfs, local, nobrowse)
map -hosts on /net (autofs, nosuid, automounted, nobrowse)
map auto_home on /home (autofs, automounted, nobrowse)
/dev/disk2s1 on /Volumes/disk_image (hfs, local, nodev, nosuid, read-only, noowners, quarantine, mounted by mikeboers)
        '''))
        self.assertEqual(mounts[0], ('/dev/disk1', '/', 'hfs', frozenset(('local', 'journaled'))))
        self.assertEqual(mounts[1], ('devfs', '/dev', 'devfs', frozenset(('local', 'nobrowse'))))
        self.assertEqual(mounts[2], ('map -hosts', '/net', 'autofs', frozenset(('nosuid', 'automounted', 'nobrowse'))))
        self.assertEqual(mounts[3], ('map auto_home', '/home', 'autofs', frozenset(('automounted', 'nobrowse'))))
        self.assertEqual(mounts[4], ('/dev/disk2s1', '/Volumes/disk_image', 'hfs', frozenset(('local', 'nodev', 'nosuid', 'read-only', 'noowners', 'quarantine', 'mounted by mikeboers'))))


    def test_linux_mounts(self):
        mounts = list(utils._parse_linux_mounts('''
/dev/sdc1 on / type ext4 (rw,errors=remount-ro)
proc on /proc type proc (rw,noexec,nosuid,nodev)
/dev/sdb on /Volumes/k1r1 type xfs (rw,noatime,filestreams,inode64)
10.0.0.222:/Volumes/k2r3 on /Volumes/k2r3 type nfs4 (rw,rsize=131072,wsize=131072,addr=10.0.0.222,clientaddr=10.0.0.221)
        '''))
        self.assertEqual(mounts[0], ('/dev/sdc1', '/', 'ext4', frozenset(('rw', 'errors=remount-ro'))))
        self.assertEqual(mounts[1], ('proc', '/proc', 'proc', frozenset(('rw', 'noexec', 'nosuid', 'nodev'))))
        self.assertEqual(mounts[2], ('/dev/sdb', '/Volumes/k1r1', 'xfs', frozenset(('rw', 'noatime', 'filestreams', 'inode64'))))
        self.assertEqual(mounts[3], ('10.0.0.222:/Volumes/k2r3', '/Volumes/k2r3', 'nfs4', frozenset(('rw', 'rsize=131072', 'wsize=131072', 'addr=10.0.0.222', 'clientaddr=10.0.0.221'))))

