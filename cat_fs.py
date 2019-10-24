#!/usr/bin/python3


import os
import sys
import errno
import re
import subprocess
import time

from fusepy import FUSE, FuseOSError, Operations
from settings import CONCAT_FILE_EXTENSION, DB_DUMP_FILENAME, EXTENDED_PATH_VALIDATION, READONLY_FLAG, VALID_PATH_REGEX


''' This object mimics a FileHeader so that the OS won't notice that the file doesn't actualy exist '''
class FileHeader(object):

    def __init__(self):
        self.header = {
            'st_ctime': time.time(),
            'st_gid': None,
            'st_mode': None,
            'st_mtime': 0,
            'st_nlink': 1,
            'st_size': 0,
            'st_uid': None
        }

    # Invokes the addfile method, first retrieving the FH for that file from its path
    def addFileFromPath(self, path):
        self.addFile(dict((key, getattr(os.lstat(path), key)) for key in ('st_atime', 'st_ctime',
                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')))

    # Reads a file's FH, and updates the header for dominant information in that FH
    def addFile(self, fh):
        if self.header['st_ctime'] > fh['st_ctime'] : self.header['st_ctime'] = fh['st_ctime']
        if self.header['st_mtime'] < fh['st_mtime'] : self.header['st_mtime'] = fh['st_mtime']
        if not self.header['st_mode'] : self.header['st_mode'] = int(fh['st_mode'])
        if not self.header['st_gid'] : self.header['st_gid'] = int(fh['st_gid'])
        if not self.header['st_uid'] : self.header['st_uid'] = int(fh['st_uid'])
        self.header['st_size'] = int(self.header['st_size'] + fh['st_size'])

    # returns the FH as a dictonairy
    def get_dict(self):
        return self.header

    def __str__(self):
        return str(self.header)


''' This is the actual FUSE FS, overwriting all the methods needed to function as a full FS '''
class Passthrough(Operations):
    def __init__(self, root, mountpoint, readonly=False):
        self.root = root
        self.mountpoint = mountpoint
        self.readonly = readonly

    # Helpers
    # =======

    # Decorator which recognizes if FUSE is invoked RO and raises appropriate error
    def _read_only(func):
        def decorator(self, *args, **kwargs) :
            if self.readonly: raise FuseOSError(errno.EROFS)
            func(self, *args, **kwargs)
        return decorator

    # Return the full path of a partial path based on the root (source path of the mointpoint))
    def _full_path(self, partial, from_mount=False):
        if partial.startswith("/"):
            partial = partial[1:]
        if from_mount:
            path = os.path.join(self.mountpoint, partial)
        else:
            path = os.path.join(self.root, partial)
        return path

    def check_if_valid_concat_path(self, path):
        return bool(os.path.isdir(path) and (not EXTENDED_PATH_VALIDATION or re.search(VALID_PATH_REGEX, path)))
    
    # return the stats for a file using its absolute path
    def get_stats_for_path(self, path):
        return dict((key, getattr(os.lstat(path), key)) for key in ('st_atime', 'st_ctime',
                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    # get a valid FD from a path, used to supply a valid FD for non-existing files.
    def get_valid_fd(self, path, flags):
        full_dir_path = path.replace(DB_DUMP_FILENAME, '/') #= path.rsplit('/', 1)[0]
        if os.path.isdir(full_dir_path):
            for f in os.listdir(full_dir_path):
                if f.endswith(CONCAT_FILE_EXTENSION):
                    return os.open(os.path.join(full_dir_path, f), flags)
        raise Exception("{} not a dir".format(full_dir_path))

        
    ''' Filesystem methods '''

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        sql_dump_fh = FileHeader()
        full_path = self._full_path(path)
        if full_path.endswith(DB_DUMP_FILENAME):
            sql_folder = full_path.replace(DB_DUMP_FILENAME, '/')
            if not self.check_if_valid_concat_path(sql_folder) : raise FuseOSError(errno.ENOSYS)
            for f in os.listdir(sql_folder):
                if f.endswith(CONCAT_FILE_EXTENSION):
                    sql_dump_fh.addFile(self.get_stats_for_path(os.path.join(sql_folder, f)))
            return sql_dump_fh.get_dict()
        return self.get_stats_for_path(full_path)

    def readdir(self, path, fh):
        sql_dumps_shown = []
        dirents = ['.', '..']
        full_path = self._full_path(path)
        if not os.path.isdir(full_path) : raise FuseOSError(errno.ENOSYS)
        dirents.extend(os.listdir(full_path))
        for r in dirents:
            r_path =  os.path.join(full_path, r)
            if self.check_if_valid_concat_path(r_path):   
                for f in os.listdir(r_path):
                    if f.endswith(CONCAT_FILE_EXTENSION) and not r in sql_dumps_shown: 
                        sql_dumps_shown.append(r)
                        yield '{}{}'.format(r, DB_DUMP_FILENAME)
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        #return pathname
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            #raise Exception(os.path.relpath(pathname, self.root, from_mount=True))
            return os.path.relpath(pathname, self.root)
        #else:
        return pathname

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    ''' Filesystem write methods '''

    @_read_only
    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    @_read_only
    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    @_read_only
    def unlink(self, path):
        return os.unlink(self._full_path(path))

    @_read_only
    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    @_read_only
    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    @_read_only
    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    @_read_only
    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    @_read_only
    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    @_read_only
    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    ''' File read methods '''

    def open(self, path, flags):
        full_path = self._full_path(path)
        if path.endswith(DB_DUMP_FILENAME):
            # Return a valid file descriptor of one of the files part of the dump
            #os.path.join(full_path, path.rsplit('/', 1)[1].replace(DB_DUMP_FILENAME, '/'))
            return self.get_valid_fd(full_path, flags)
        return os.open(full_path, flags)

    def read(self, path, length, offset, fh):
        # Return the concatenated file, if it is read
        if path.endswith(DB_DUMP_FILENAME):
            full_dir_path = self._full_path(path).replace(DB_DUMP_FILENAME, '/')
            command = ['cat']
            if self.check_if_valid_concat_path(full_dir_path):
                for f in os.listdir(full_dir_path):
                    if f.endswith(CONCAT_FILE_EXTENSION):
                        command.append(os.path.join(full_dir_path, f))
            return subprocess.run(command, stdout=subprocess.PIPE).stdout[offset:offset+length]
        # Otherwise just passtrough the read 
        else:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

    ''' File write methods '''

    @_read_only
    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    @_read_only
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    @_read_only
    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)


def main(root, mountpoint, readonly):
    FUSE(Passthrough(root, mountpoint, readonly), mountpoint, nothreads=True, foreground=True, allow_other=True)

if __name__ == '__main__':
    try:
        try:
            if READONLY_FLAG in sys.argv[3].lower() : readonly = True
        except IndexError as e:
            readonly = False
        main(sys.argv[1], sys.argv[2], readonly)
    except IndexError as e:
        print("Usage:\n    python3 cat_fs.py [source_path] [destination_mount_path] ([readonly, --readonly])")
