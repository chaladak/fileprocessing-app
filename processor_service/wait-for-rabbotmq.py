#!/usr/bin/env python3
import time
import socket
import sys
import os

def is_rabbitmq_ready(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def main():
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.environ.get('RABBITMQ_PORT', 5672))
    max_retries = int(os.environ.get('RABBITMQ_WAIT_RETRIES', 30))
    retry_interval = int(os.environ.get('RABBITMQ_WAIT_INTERVAL', 2))

    print(f"Waiting for RabbitMQ to be ready at {rabbitmq_host}:{rabbitmq_port}...")
    
    for i in range(max_retries):
        if is_rabbitmq_ready(rabbitmq_host, rabbitmq_port):
            print("RabbitMQ is ready!")
            return 0
        
        print(f"Attempt {i+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)
    
    print(f"Could not connect to RabbitMQ after {max_retries} attempts. Exiting.")
    return 1

if __name__ == '__main__':
    sys.exit(main())