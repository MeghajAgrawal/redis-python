# Uncomment this to pass the first stage
import socket

def handle_connection(client_socket):
    with client_socket:
        recv = client_socket.recv(1024)
        client_socket.sendall("+PONG\r\n".encode())

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket,addr = server_socket.accept() # wait for client
    handle_connection(client_socket)


if __name__ == "__main__":
    main()
