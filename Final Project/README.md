## To start Server:
```bash
python Server.py 8080
```
## To start the File System:
```bash
python FileSystem.py fusemount
```
## To unmount file system:
```bash
fusermount -uz ./fusemount
```
Make sure you are outside of the mount point when unmounting

## To inspect a running server:
```bash
python test/insepct_server.py 8080
```