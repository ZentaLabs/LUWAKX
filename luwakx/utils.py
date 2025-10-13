import platform
import subprocess

def has_gpu():
    system = platform.system()
    try:
        if system in ("Linux", "Windows"):
            return subprocess.call("nvidia-smi", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
        elif system == "Darwin":
            out = subprocess.check_output(["system_profiler", "SPDisplaysDataType"])
            return b"Metal" in out
    except Exception:
        return False
    return False
