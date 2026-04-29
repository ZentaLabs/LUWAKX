import platform
import subprocess
import requests
import psutil


def cleanup_lm_studio_workers():
    """
    Kill LM Studio worker processes while keeping the main server running.
    
    This function identifies and terminates worker processes spawned by
    LM Studio server after handling inference requests. These workers are
    identical to the main server process (same executable path) but have
    higher PIDs since they're created later.
    
    Workers are only killed if their GPU memory usage is between 1000 MiB
    and 5000 MiB, which indicates they're holding inference artifacts.
    
    The main LM Studio server (lowest PID) is preserved so the model stays
    loaded and the server remains available for subsequent requests.
    
    Safe to call even if LM Studio is not running or psutil is not available.
    """
    try:
        # Find all LM Studio internal Node.js processes
        lm_processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'exe']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                exe_path = proc.info.get('exe', '')
                
                # Skip if not a Node.js process
                if 'node' not in proc.info['name'].lower():
                    continue
                
                # Check if this is an LM Studio internal process
                is_lm_internal = (
                    '.lmstudio' in exe_path.lower() and '.internal' in exe_path.lower()
                ) or (
                    '.lmstudio' in cmdline.lower() and '.internal' in cmdline.lower()
                )
                
                if is_lm_internal:
                    lm_processes.append(proc)
                        
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        # If we found multiple LM Studio processes, check GPU memory and kill workers
        killed_count = 0
        if len(lm_processes) > 1:
            # Sort by PID (ascending)
            lm_processes.sort(key=lambda p: p.info['pid'])
            
            # Get GPU memory usage for each process (platform-specific)
            pid_to_gpu_mem = {}
            system = platform.system()
            
            if system in ("Linux", "Windows"):
                # Use nvidia-smi for NVIDIA GPUs
                try:
                    result = subprocess.run(
                        ['nvidia-smi', '--query-compute-apps=pid,used_memory', '--format=csv,noheader,nounits'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    if result.returncode == 0:
                        for line in result.stdout.strip().split('\n'):
                            if line.strip():
                                parts = line.split(',')
                                if len(parts) == 2:
                                    pid = int(parts[0].strip())
                                    gpu_mem_mib = float(parts[1].strip())
                                    pid_to_gpu_mem[pid] = gpu_mem_mib
                except Exception:
                    pass  # nvidia-smi not available or failed
            
            # Kill workers (all except first) if GPU memory is in range
            # On macOS or if GPU check failed, kill all workers without memory check
            for proc in lm_processes[1:]:
                try:
                    pid = proc.info['pid']
                    gpu_mem = pid_to_gpu_mem.get(pid, 0)
                    
                    # If we have GPU memory info, only kill if in range
                    # Otherwise (macOS or GPU check failed), kill all workers
                    if pid_to_gpu_mem:
                        if 1000 <= gpu_mem <= 5000:
                            proc.kill()
                            killed_count += 1
                    else:
                        pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                
        return killed_count
        
    except ImportError:
        pass  # psutil not installed, skip cleanup
    except Exception:
        pass  # Silently ignore errors during cleanup
    
    return 0


def cleanup_gpu_memory():
    """
    Release GPU memory allocated by PyTorch/CUDA.
    
    Clears the GPU cache and forces garbage collection to free up
    memory for other processes. Safe to call even if PyTorch is not
    installed or GPU is not available.
    
    This is useful after GPU-intensive operations (like ML face detection)
    to ensure memory is available for other applications.
    """
    try:
        import torch
        import gc
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
        gc.collect()
    except ImportError:
        pass  # PyTorch not installed, skip GPU cleanup
    except Exception:
        pass  # Silently ignore errors during cleanup


def has_gpu():
    """
    Detect if a compatible GPU is available on the system.

    Returns:
        bool: True if a GPU is detected (NVIDIA on Linux/Windows, Metal on macOS), False otherwise.
    """
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


def download_github_asset_by_tag(owner, repo, tag, asset_name, dest_path, token):
    """
    Download a specific asset from a GitHub release by tag name.

    Args:
        owner (str): GitHub repository owner (organization or user).
        repo (str): Repository name.
        tag (str): Release tag to fetch (e.g., 'v1.0.0' or 'latest').
        asset_name (str): Name of the asset file to download.
        dest_path (str): Local file path to save the downloaded asset.
        token (str): GitHub personal access token for authentication (must have repo access).

    Returns:
        bool: True if the asset was downloaded successfully.

    Raises:
        RuntimeError: If the asset is not found in the specified release.
    """
    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    release = requests.get(api_url, headers=headers).json()
    for asset in release.get("assets", []):
        if asset["name"] == asset_name:
            asset_url = asset["url"]
            download_headers = {"Accept": "application/octet-stream"}
            if token:
                download_headers["Authorization"] = f"token {token}"
            r = requests.get(asset_url, headers=download_headers)
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                f.write(r.content)
            return True
    raise RuntimeError(f"Asset {asset_name} not found in release {tag}")
