import argparse
import socket
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('name', nargs=1, default='node', type=str)
parser.add_argument('address', nargs=1, default='localhost', type=str)
parser.add_argument('port', nargs=1, type=int)

def main():
    args = parser.parse_args()
    name = args.name[0]
    host = args.address[0]
    port = args.port[0]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))

        while True:
            for line in sys.stdin:
                s.sendall(str.encode(f'{name} {line}'))


if __name__ == "__main__":
    main()
