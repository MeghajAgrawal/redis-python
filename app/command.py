import time
from dataclasses import dataclass


@dataclass
class Constant:
    NULL_BULK_STRING = "$-1\r\n"
    CLRF = "\r\n"
    TERMINATOR = b"\r\n"
    REPL = "replication"  

@dataclass
class Command:
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"
    REPLCONF = "replconf"
    PSYNC = "psync"

@dataclass
class CommandProperties:
    ROLE = None
    HOST = None
    PORT = None
    MASTER_REPLID = None
    MASTER_REPL_OFFSET = None
    MASTER_HOST = None
    MASTER_PORT = None

data_store = {}

def receive_server_properties(server_properties):
    CommandProperties.ROLE = server_properties.ROLE
    CommandProperties.HOST = server_properties.HOST
    CommandProperties.PORT = server_properties.PORT
    CommandProperties.MASTER_REPLID = server_properties.MASTER_REPLID
    CommandProperties.MASTER_REPL_OFFSET = server_properties.MASTER_REPL_OFFSET
    CommandProperties.MASTER_HOST = server_properties.MASTER_HOST
    CommandProperties.MASTER_PORT = server_properties.MASTER_PORT

def command_decoder(data):
    input_request = data.split(Constant.CLRF)
    print(input_request)
    if input_request[-1] == "":
        input_request.pop()
    if len(input_request) < 3:
        raise Exception("Invalid Command")
    return input_request

def response_handler(data):
    input_request = command_decoder(data)
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
        
        case Command.INFO:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            if input_request[4].lower() == Constant.REPL:
                if CommandProperties.ROLE == "master":
                    msg = f"role:{CommandProperties.ROLE}master_replid:{CommandProperties.MASTER_REPLID}master_repl_offset:{CommandProperties.MASTER_REPL_OFFSET}"
                else:
                    msg = f"role:{CommandProperties.ROLE}"
                return f"${len(msg)}\r\n{msg}\r\n"
        
        case Command.REPLCONF:
            if len(input_request)<5:
                raise Exception("Invalid Command")
            return f"+OK\r\n" 

        case Command.PSYNC:
            if len(input_request) < 7:
                raise Exception("Invalid Command")
            repl_id = input_request[4]
            return f"+FULLRESYNC {repl_id} 0\r\n"