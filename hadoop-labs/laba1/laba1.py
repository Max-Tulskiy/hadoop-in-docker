import logging
import argparse
from hdfs import InsecureClient


def find_dir_number(path: str, hdfs_client):
    logging.info("Start counting directories in path")
    
    dir_number = 0
    
    try:
        content = hdfs_client.list(path)

        for elem in content:
            if hdfs_client.status(f'{path}/{elem}')['type'] == "DIRECTORY":
                dir_number += 1
        logging.info("Success while counting dirs")

        return dir_number

    except Exception:
        logging.error("Error while counting dirs")
        return None


def main():
    logging.basicConfig(level=logging.INFO)

    hadoop_url = "http://localhost:9870"

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path")
    args = parser.parse_args()
    path = args.path
    
    hdfs_client = InsecureClient(hadoop_url)
    
    dir_number = find_dir_number(path, hdfs_client)

    logging.info(f"Number of directories: {dir_number}")


if __name__ == "__main__":
    main()