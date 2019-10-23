# con-fuse-cat
An implementation of the FUSE filesystem using fusepy which concatenates files in a directory and displays them as a single file in a virtual filesystem.

## Directory overview
```
    |-README.md
    |-cat_fs.py         #main program
    |-settings.py       #settings/configuration file
    |-requirements.txt  #python depencies listing
```

### OS dependencies
 - FUSE
 - Python3.5+
 - Virtualenv (if used)


### Installing Python dependencies
 - 0. (Optional) Create VirtualEnv && activate: `virtualenv venv; source venv/bin/activate` (This encapsulates all Python dependencies)
 - 1. Install python dependencies/requirements: `pip3 install -r requirements.txt`


### Running the filesystem
`python3 cat_fs.py [source_path] [destination_mount_path] ([readonly, --readonly])`