# Uncomment this to pass the first stage
import socket
import threading
import time
from dataclasses import dataclass


@dataclass
class Constant:
    NULL_BULK_STRING = "$-1\r\n"
    CLRF = "\r\n"
    TERMINATOR = b"\r\n"

@dataclass
class Command:
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"

data_store = {}

def handle_connection(client_socket, addr):
    with client_socket:
        while True:
            data = client_socket.recv(1024)
            if not data:
                print("client at", addr, " broke connection")
                break
            #client_socket.send("+PONG\r\n".encode())
            try:
                input_request = command_decoder(data.decode())
                msg = response_handler(input_request)
                client_socket.send(msg.encode())
            except Exception as e:
                print(e)

def command_decoder(data):
    input_request = data.split(Constant.CLRF)
    print(input_request)
    if input_request[-1] == "":
        input_request.pop()
    if len(input_request) < 3:
        raise Exception("Invalid Command")
    return input_request

def response_handler(input_request):
    command = input_request[2]
    match command.lower():
        case Command.PING:
            return "+PONG\r\n"
        
        case Command.ECHO:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            msg = input_request[4]
            return f"${len(msg)}\r\n{input_request[4]}\r\n"
        
        case Command.SET:
            if len(input_request) < 7:
                raise Exception("Invalid Command")
            key = input_request[4]
            value = input_request[6]
            data_store[key] = {"value" : value, "px": None}
            if len(input_request) > 7:
                px_time = int(input_request[10])
                data_store[key]["px"] = time.time() * 1000 + px_time
            return f"+OK\r\n"
        
        case Command.GET:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            key = input_request[4]
            current_time = time.time() * 1000
            if key in data_store:
                if not data_store[key]["px"] or current_time < data_store[key]["px"]:
                    value = data_store[key]["value"]
                    return f"${len(value)}\r\n{value}\r\n"
                data_store.pop(key)
            return Constant.NULL_BULK_STRING
            

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
