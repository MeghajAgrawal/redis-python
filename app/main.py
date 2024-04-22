# Uncomment this to pass the first stage
import socket
import threading

def handle_connection(client_socket, addr):
    with client_socket:
        while True:
            data = client_socket.recv(1024)
            if not data:
                print("client at", addr, " broke connection")
                break
            client_socket.send("+PONG\r\n".encode())

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
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
