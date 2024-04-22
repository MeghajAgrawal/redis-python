# Uncomment this to pass the first stage
import socket
import threading
from argparse import ArgumentParser
from dataclasses import dataclass

from app import command


@dataclass
class Constant:
    DEFAULT_PORT = 6379
    DEFAULT_HOST = "localhost"
    MASTER = "master"
    SLAVE = "slave"

@dataclass
class ServerProperties:
    ROLE = None
    HOST = None
    PORT = None
    MASTER_REPLID = None
    MASTER_REPL_OFFSET = None
    MASTER_HOST = None
    MASTER_PORT = None

def handle_connection(client_socket, addr):
    with client_socket:
        while True:
            data = client_socket.recv(1024)
            if not data:
                print("client at", addr, " broke connection")
                break
            #client_socket.send("+PONG\r\n".encode())
            try:
                msg = command.response_handler(data.decode())
                client_socket.send(msg.encode())
            except Exception as e:
                print(e)        

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    parser = ArgumentParser()
    parser.add_argument("--port", type= int, default=Constant.DEFAULT_PORT)
    parser.add_argument("--replicaof", nargs= 2)
    parser_args = parser.parse_args()
    server_address  = (Constant.DEFAULT_HOST, parser_args.port)

    ServerProperties.HOST = Constant.DEFAULT_HOST
    ServerProperties.PORT = parser_args.port
    ServerProperties.ROLE = Constant.MASTER
    ServerProperties.MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    ServerProperties.MASTER_REPL_OFFSET = 0

    if parser_args.replicaof is not None:
        ServerProperties.ROLE = Constant.SLAVE
        ServerProperties.MASTER_HOST = parser_args.replicaof[0]
        ServerProperties.MASTER_PORT = parser_args.replicaof[1]
    
    command.receive_server_properties(ServerProperties)

    server_socket = socket.create_server(server_address, reuse_port=True)
    while True:
        try:
            (client_socket,addr) = server_socket.accept() # wait for client
            thread = threading.Thread(target=handle_connection, args= (client_socket,addr))
            thread.start()
            #handle_connection(client_socket)
        except Exception as e:
            print("Exception occurred:", e)

if __name__ == "__main__":
    main()
