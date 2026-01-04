import os
import platform

def get_file_id(path: str):
    """Return a platform-dependent identifier for a file.

    This function generates a tuple that can be used to identify a file
    across executions, using different filesystem attributes depending
    on the operating system. The returned identifier is intended for
    lightweight file identity checks (e.g. change detection), not for
    cryptographic or absolute uniqueness guarantees.

    Platform-specific behavior:

    - Linux / macOS (Darwin): Uses inode number and device ID.
    - Windows: Uses file size and creation time.
    - Other platforms: Uses file size and last modification time.

    Args:
        path (str): Path to the file.

    Returns:
        tuple: A tuple of filesystem attributes that together act as a
        best-effort file identifier. The tuple structure depends on the
        operating system.

    Raises:
        FileNotFoundError: If the file does not exist.
        PermissionError: If the file cannot be accessed.
        OSError: For other operating system-related errors.
    
    """
    stat = os.stat(path)
    system = platform.system()

    if system in ("Linux", "Darwin"):
        return (stat.st_ino, stat.st_dev)
    elif system == "Windows":
        return (stat.st_size, stat.st_birthtime)
    else:
        return (stat.st_size, stat.st_mtime)

