#!/usr/bin/python3


import os
import sys
import errno
import subprocess
import time

from fuse import FUSE, FuseOSError, Operations

DB_DUMP_FILENAME = 'DB_DUMP'

# DB_DUMPS = 

# This object mimics a FileHeader so that the OS won't notice that the file doesn't actualy exist
class FileHeader(object):

    def __init__(self):
        self.header = {
            'st_ctime': time.time(),
            'st_gid': None,
            'st_mode': 33261,
            'st_mtime': 0,
            'st_nlink': 1,
            'st_size': 0,
            'st_uid': None
        }


    def addFileFromPath(self, path):
        self.addFile(dict((key, getattr(os.lstat(path), key)) for key in ('st_atime', 'st_ctime',
                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')))
    
    def addFile(self, fh):
        if self.header['st_ctime'] > fh['st_ctime'] : self.header['st_ctime'] = fh['st_ctime']
        if self.header['st_mtime'] < fh['st_mtime'] : self.header['st_mtime'] = fh['st_mtime'] 
        if not self.header['st_gid'] : self.header['st_gid'] = int(fh['st_gid'])
        if not self.header['st_uid'] : self.header['st_uid'] = int(fh['st_uid'])
        self.header['st_size'] = int(self.header['st_size'] + fh['st_size'])

    def get_dict(self):
        return self.header

    def __str__(self):
        return str(self.header)



class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # return the stats for a file using its absolute path
    def get_stats_for_path(self, path):
        return dict((key, getattr(os.lstat(path), key)) for key in ('st_atime', 'st_ctime',
                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    # get a valid FD 
    def get_valid_fd(self, path, flags):
        full_dir_path = path.rsplit('/', 1)[0]
        if os.path.isdir(full_dir_path):
            for f in os.listdir(full_dir_path):
                if f.endswith('.sql'):
                    return os.open(os.path.join(full_dir_path, f), flags) 

        
    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        sql_dump_fh = FileHeader()
        full_path = self._full_path(path)
        if full_path.endswith(DB_DUMP_FILENAME):
            files = os.listdir(full_path.rsplit('/', 1)[0])
            for f in files:
                if f.endswith('.sql'):
                    sql_dump_fh.addFile(self.get_stats_for_path(os.path.join(full_path.rsplit('/', 1)[0], f)))
            return sql_dump_fh.get_dict()
        return self.get_stats_for_path(full_path)

    def readdir(self, path, fh):
        full_path = self._full_path(path)
        sql_dump_shown = False

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            if r.endswith('.sql') and not sql_dump_shown: # replace with better selector when refactoring to sepereate folder
                yield DB_DUMP_FILENAME
                sql_dump_shown = True
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        if path.endswith(DB_DUMP_FILENAME):
            # Return a valid file descriptor of one of the files part of the dump
            return self.get_valid_fd(full_path, flags)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        # Return the concatenated file, if it is read
        if path.endswith(DB_DUMP_FILENAME):
            full_dir_path = self._full_path(path).rsplit('/', 1)[0]
            command = ['cat']
            if os.path.isdir(full_dir_path):
                for f in os.listdir(full_dir_path):
                    if f.endswith('.sql'):
                        command.append(os.path.join(full_dir_path, f))
            return subprocess.run(command, stdout=subprocess.PIPE).stdout[offset:offset+length]
        # Otherwise just passtrough the read 
        else:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(root, mountpoint):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    try:
        main(sys.argv[1], sys.argv[2])
    except Exception as e:
        print("Usage:\n    python3 cat_fs.py [source_path] [destination_mount_path]")
