import hdfs


def upload() -> None:
    client = hdfs.InsecureClient(
        url="http://localhost:9870", user="hadoop"
    )  # http://localhost:50070

    # datanode_client = InsecureClient(
    #    url="http://localhost:9864", user="hadoop"
    # )

    client.makedirs("/user/lab")
    files = client.list("/user/lab")
    print(files)
    # print(datanode_client.list("/user"))
    # datanode_client.upload('/user/lab', 'src/spark_lab/u.data')
    # client.upload('/user/lab', 'src/spark_lab/u.item')
    # with open("src/spark_lab/u.data", "rb") as F:
    #     u_data = F.read()
    # with client.write("/user/lab/u.data") as writer:
    #     writer.write(u_data)
    # with client.read("/user/lab/u.data") as reader:
    #     print(reader.read())


if __name__ == "__main__":
    upload()
