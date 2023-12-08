import subprocess


def mkdirs(directory: str) -> None:
    bin_path = "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs"
    cmd = f"{bin_path} dfs -mkdir -p {directory}"
    subprocess.run(cmd)


def load() -> None:
    # mkdirs("/user/")
    # mkdirs("/user/lab/")

    local_path = "/mnt/d/Projects1/PyProjects/spark-lab/src/spark_lab/u.data"
    hdfs_path = "/user/lab/"
    bin_path = "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs"
    cmd = f"{bin_path} dfs -copyFromLocal {local_path} {hdfs_path}"
    subprocess.run(cmd, check=True, shell=True)


if __name__ == "__main__":
    load()
