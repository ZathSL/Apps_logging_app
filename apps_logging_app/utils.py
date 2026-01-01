import os
import platform

def get_file_id(path: str):
    """
    Return a tuple of file identifiers for the given path.

    On Linux/Darwin, the identifiers are the inode and device number.
    On Windows, the identifiers are the file size and creation time.
    On other systems, the identifiers are the file size and last modification time.

    :param path: the path to the file
    :return: a tuple of file identifiers
    :rtype: tuple
    """
    stat = os.stat(path)
    system = platform.system()

    if system in ("Linux", "Darwin"):
        return (stat.st_ino, stat.st_dev)
    elif system == "Windows":
        return (stat.st_size, stat.st_ctime)
    else:
        return (stat.st_size, stat.st_mtime)

