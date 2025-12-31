import os
import platform

def get_file_id(path: str):
    stat = os.stat(path)
    system = platform.system()

    if system in ("Linux", "Darwin"):
        return (stat.st_ino, stat.st_dev)
    elif system == "Windows":
        return (stat.st_size, stat.st_ctime)
    else:
        return (stat.st_size, stat.st_mtime)

