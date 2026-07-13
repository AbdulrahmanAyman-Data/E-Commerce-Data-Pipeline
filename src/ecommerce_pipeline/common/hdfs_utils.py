import os
import subprocess
from typing import List
from ecommerce_pipeline.common.logger import get_logger

logger = get_logger(__name__)

HADOOP_CONTAINER = "hadoop-master-eco"
HADOOP_USER = "hdfs"

IN_DOCKER = os.path.exists("/.dockerenv")


def _run_hdfs(args: List[str]) -> subprocess.CompletedProcess:
    
    if IN_DOCKER:
        
        cmd = ["hdfs"] + args
    else:
        cmd = ["docker", "exec", "-u", HADOOP_USER, HADOOP_CONTAINER, "hdfs"] + args
    return subprocess.run(cmd, capture_output=True, text=True)


def hdfs_mkdir(path: str) -> bool:
    result = _run_hdfs(["dfs", "-mkdir", "-p", path])
    if result.returncode != 0:
        logger.error(f"hdfs_mkdir failed: {result.stderr}")
    return result.returncode == 0
def hdfs_exists(path: str) -> bool:
    result = _run_hdfs(["dfs", "-test", "-e", path])
    return result.returncode == 0


def hdfs_put(local_path: str, hdfs_path: str, overwrite: bool = True) -> bool:
    if IN_DOCKER:
         
        cmd = ["dfs", "-put"]
        if overwrite:
            cmd.append("-f")
        cmd += [local_path, hdfs_path]
        result = _run_hdfs(cmd)
        if result.returncode != 0:
            logger.error(f"hdfs_put failed: {result.stderr}")
        return result.returncode == 0
    else:
        
        import os
        filename = os.path.basename(local_path)
        container_tmp = f"/tmp/{filename}"

        cp_result = subprocess.run(
            ["docker", "cp", local_path, f"{HADOOP_CONTAINER}:{container_tmp}"],
            capture_output=True, text=True
        )
        if cp_result.returncode != 0:
            logger.error(f"docker cp failed: {cp_result.stderr}")
            return False

        cmd = ["dfs", "-put"]
        if overwrite:
            cmd.append("-f")
        cmd += [container_tmp, hdfs_path]
        result = _run_hdfs(cmd)
        if result.returncode != 0:
            logger.error(f"hdfs_put failed: {result.stderr}")

        subprocess.run(
            ["docker", "exec", HADOOP_CONTAINER, "rm", container_tmp],
            capture_output=True
        )
        return result.returncode == 0


def hdfs_ls(path: str) -> List[str]:
    result = _run_hdfs(["dfs", "-ls", path])
    if result.returncode != 0:
        return []
    lines = result.stdout.strip().split("\n")[1:]
    return [line.split()[-1] for line in lines if line]