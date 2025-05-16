#!/usr/bin/env python3
import time
import socket
import sys
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_rabbitmq_ready(host, port):
    """
    Check if RabbitMQ is available on the specified host and port.
    
    Args:
        host (str): The hostname or IP address of the RabbitMQ server
        port (int): The port number of the RabbitMQ server
        
    Returns:
        bool: True if connection succeeded, False otherwise
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.warning(f"Socket connection error: {e}")
        return False

def get_rabbitmq_connection_params():
    """
    Construct RabbitMQ connection parameters from environment variables.
    
    Returns:
        tuple: (host, port) for RabbitMQ connection
    """
    # Get connection details from environment variables with defaults
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    
    if rabbitmq_url and '@' in rabbitmq_url:
        # Extract host from RABBITMQ_URL if it exists
        try:
            host_part = rabbitmq_url.split('@')[1].split('/')[0]
            if ':' in host_part:
                host, port_str = host_part.split(':')
                port = int(port_str)
            else:
                host = host_part
                port = 5672
                
            logger.info(f"Using RabbitMQ host:port from RABBITMQ_URL: {host}:{port}")
            return host, port
        except Exception as e:
            logger.warning(f"Failed to parse RABBITMQ_URL: {e}, falling back to individual variables")
    
    # Use individual environment variables if URL parsing failed or URL not provided
    host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    port = int(os.environ.get('RABBITMQ_PORT', 5672))
    logger.info(f"Using RabbitMQ host:port from individual env vars: {host}:{port}")
    
    return host, port

def main():
    """
    Main function that periodically checks RabbitMQ availability and exits
    when available or after the maximum number of retries.
    
    Returns:
        int: 0 if RabbitMQ becomes available, 1 if maximum retries exceeded
    """
    # Get connection parameters
    rabbitmq_host, rabbitmq_port = get_rabbitmq_connection_params()
    
    # Get retry parameters
    max_retries = int(os.environ.get('RABBITMQ_WAIT_RETRIES', 30))
    retry_interval = int(os.environ.get('RABBITMQ_WAIT_INTERVAL', 5))

    logger.info(f"Waiting for RabbitMQ to be ready at {rabbitmq_host}:{rabbitmq_port}...")
    
    for attempt in range(max_retries):
        if is_rabbitmq_ready(rabbitmq_host, rabbitmq_port):
            logger.info("RabbitMQ is ready!")
            return 0
        
        logger.info(f"Attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)
    
    logger.error(f"Could not connect to RabbitMQ after {max_retries} attempts. Exiting.")
    return 1

if __name__ == '__main__':
    sys.exit(main())