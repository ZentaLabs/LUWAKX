import platform
import subprocess
import requests


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
    headers = {"Authorization": f"token {token}"}
    release = requests.get(api_url, headers=headers).json()
    for asset in release.get("assets", []):
        if asset["name"] == asset_name:
            asset_url = asset["url"]
            download_headers = {
                "Authorization": f"token {token}",
                "Accept": "application/octet-stream"
            }
            r = requests.get(asset_url, headers=download_headers)
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                f.write(r.content)
            return True
    raise RuntimeError(f"Asset {asset_name} not found in release {tag}")
