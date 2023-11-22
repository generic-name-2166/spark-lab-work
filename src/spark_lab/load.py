from hdfs import InsecureClient


def upload() -> None:
    client = InsecureClient(url="http://localhost:50070", user="hadoop")

    client.makedirs('/user/lab')
    client.upload('/user/lab', 'u.data')


if __name__ == "__main__":
    upload()
