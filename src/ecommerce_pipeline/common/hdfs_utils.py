import subprocess
from ecommerce_pipeline.common.config_loader import load_config

def _get_namenode() -> str:
    config = load_config()
    return config.get("hdfs", {}).get("namenode_uri", "hdfs://master:/9000")


def hdfs_mkdir(path: str) -> bool:
    full = f"{_get_namenode()}{path}"
    result = subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", full],
        capture_output=True , text=True
    )
    return result.returncode == 0



def hdfs_exists(path: str) -> bool:
    full = f"{_get_namenode()}{path}"
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", full],
        capture_output=True , text=True
    )
    return result.returncode == 0


def hdfs_put(local_path: str, hdfs_path: str, overwrite: bool = True) ->bool:
    full = f"{_get_namenode()}{hdfs_path}"
    cmd = ["hdfs", "dfs", "-put"]
    if overwrite:
        cmd.append("-f")
    cmd +=[local_path, full]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0

def hdfs_ls(path: str) -> list[str]:
    full = f"{_get_namenode}{path}"
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", full],
        capture_output=True , text=True
    )
    if result.returncode != 0:
        return []
    lines = result.stdout.strip().split("\n")[1:]  # skip header
    return [line.split()[-1] for line in lines if line]