import configparser
import os
import re
import sys
import threading

from pathlib import Path


if __name__ == "__main__":
    config_path = Path.cwd() / "naxos.ini"
    host = None
    port = None
    naxos_path = None
    
    if len(sys.argv) == 1 and config_path.is_file():  # try to read the provided config file
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            host = config['naxos']['host_ip']
            port = config['naxos']['host_port']
            naxos_path = Path(config['naxos']['naxos_directory'])

        except Exception as e:
            print('Malformatted ini file.')
            exit(0)
    else:  # try to read the necessary parameters from args
        try:
            host, port = sys.argv[1].split(":")
            naxos_path = Path(sys.argv[2])
        except:
            print('Usage: python client.py <naxos peer host>:<naxos peer port> <"path to naxos directory">')
            print('Alternative: python client.py with a naxos.ini file in the same directory')
            exit(0)

    # sanity check
    pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'  # ipv4
    if not re.match(pattern, host):
        print('host ip is not a well formed ip address.')
        exit(0)
    if not naxos_path.is_dir():
        print("provided naxos directory is not a directory.")
        exit(0)
    if not port.isdigit():
        print("provided port is not a valid positive number.")
        exit(0)
    elif not (1024 <= int(port) <= 65535):
        print("provided port is not a valid port.")
        exit(0)

    port = int(port)

    print("%s:%s" % (host, port))
    print(naxos_path)
