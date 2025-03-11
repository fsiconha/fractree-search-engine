#!/usr/bin/env python3
"""
This script uses the Docker SDK to start an Elasticsearch container
using Docker. It checks for an existing container named "elasticsearch_instance"
and restarts it if found. Otherwise, it creates a new container.
"""

import docker
import time
from docker.errors import NotFound, APIError

def start_elasticsearch_container():
    client = docker.from_env()

    container_name = "elasticsearch_instance"
    image = "docker.elastic.co/elasticsearch/elasticsearch:7.17.10"
    
    try:
        # Check if the container already exists
        container = client.containers.get(container_name)
        print("Elasticsearch container already exists. Restarting container...")
        container.restart()
    except NotFound:
        print("Creating a new Elasticsearch container...")
        try:
            container = client.containers.run(
                image=image,
                name=container_name,
                ports={'9200/tcp': 9200, '9300/tcp': 9300},
                environment={
                    "discovery.type": "single-node",
                    "ES_JAVA_OPTS": "-Xms512m -Xmx512m"
                },
                detach=True
            )
        except APIError as err:
            print(f"Error creating container: {err}")
            return None

    # Wait until Elasticsearch is up and running
    print("Waiting for Elasticsearch to start...")
    time.sleep(10)  # Adjust if necessary
    print("Elasticsearch container is running.")
    return container

if __name__ == '__main__':
    container = start_elasticsearch_container()
    if container:
        # Optionally, print the last few log lines
        logs = container.logs(tail=10).decode('utf-8')
        print("Container logs:")
        print(logs)
