import time
from dataclasses import dataclass
import base64
import socket

RDB_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

@dataclass
class Constant:
    NULL_BULK_STRING = b"$-1\r\n"
    CLRF = "\r\n"
    TERMINATOR = b"\r\n"
    REPL = "replication" 
    MASTER = "master"
    SLAVE = "slave"

@dataclass
class Command:
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"
    REPLCONF = "replconf"
    PSYNC = "psync"
    GETACK = "getack"
    ACK = "ack"

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
replicas = set()

command_array = set(Command.SET)

@dataclass
class Offset:
    current_offset = 0
    offset = 0

def receive_server_properties(server_properties):
    CommandProperties.ROLE = server_properties.ROLE
    CommandProperties.HOST = server_properties.HOST
    CommandProperties.PORT = server_properties.PORT
    CommandProperties.MASTER_REPLID = server_properties.MASTER_REPLID
    CommandProperties.MASTER_REPL_OFFSET = server_properties.MASTER_REPL_OFFSET
    CommandProperties.MASTER_HOST = server_properties.MASTER_HOST
    CommandProperties.MASTER_PORT = server_properties.MASTER_PORT

def encode(msg):
    if type(msg) is str:
        return msg.encode()
    return msg

def command_decoder(data):
    #print(data)
    input_request = data.split(Constant.CLRF)
    #print("Incoming Request", input_request)
    if input_request[-1] == "":
        input_request.pop()
    if len(input_request) < 3:
        raise Exception("Invalid Command")
    return input_request

def is_master(conn,msg):
    if CommandProperties.ROLE == Constant.MASTER:
        return conn.send(msg)


def response_handler(data,conn):
    Offset.offset = Offset.current_offset
    print(Offset.offset)
    input_request = command_decoder(data)
    command = input_request[2]
    Offset.current_offset += len(data)
    match command.lower():
        case Command.PING:
            return is_master(conn,encode("+PONG\r\n"))
        case Command.ECHO:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            msg = input_request[4]
            return is_master(conn, encode(f"${len(msg)}\r\n{input_request[4]}\r\n"))
        
        case Command.SET:
            if len(input_request) < 7:
                raise Exception("Invalid Command")
            #print("Inside SET Command ", CommandProperties.ROLE, CommandProperties.PORT)
            key = input_request[4]
            value = input_request[6]
            data_store[key] = {"value" : value, "px": None}
            if len(input_request) > 7:
                px_time = int(input_request[10])
                data_store[key]["px"] = time.time() * 1000 + px_time
            if CommandProperties.ROLE == Constant.MASTER:
                #print("Current Replicas to send ", replicas)
                for replica in replicas:
                    replica.send(data.encode())
                return conn.send(encode(f"+OK\r\n"))
        
        case Command.GET:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            #print("Current Role", CommandProperties.ROLE)
            #print("Current Data Store ", data_store)
            key = input_request[4]
            current_time = time.time() * 1000
            if key in data_store:
                if not data_store[key]["px"] or current_time < data_store[key]["px"]:
                    value = data_store[key]["value"]
                    return conn.send(encode(f"${len(value)}\r\n{value}\r\n"))
                data_store.pop(key)
            return conn.send(Constant.NULL_BULK_STRING)
        
        case Command.INFO:
            if len(input_request) < 5:
                raise Exception("Invalid Command")
            if input_request[4].lower() == Constant.REPL:
                if CommandProperties.ROLE == "master":
                    msg = f"role:{CommandProperties.ROLE}master_replid:{CommandProperties.MASTER_REPLID}master_repl_offset:{CommandProperties.MASTER_REPL_OFFSET}"
                else:
                    msg = f"role:{CommandProperties.ROLE}"
                return conn.send(encode(f"${len(msg)}\r\n{msg}\r\n"))
        
        case Command.REPLCONF:
            if len(input_request)<3:
                raise Exception("Invalid Command")
            #print("INSIDE REPLCONF" , CommandProperties.ROLE)
            #print("input request", input_request)
            if input_request[4].lower() == Command.GETACK:
                Offset.current_offset += 3
                if CommandProperties.ROLE == Constant.SLAVE:
                    #print("INSIDE SLAVE CONN SEND ACK")
                    conn.send(encode(f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(Offset.offset))}\r\n{Offset.offset}\r\n"))
            if input_request[4] == Command.ACK:
                offset = input_request[7]
                CommandProperties.MASTER_REPL_OFFSET = offset
            if CommandProperties.ROLE == Constant.MASTER:
                return conn.send(encode(f"+OK\r\n"))
        
        case Command.PSYNC:
            if len(input_request) < 7:
                raise Exception("Invalid Command")
            repl_id = CommandProperties.MASTER_REPLID
            conn.send(encode(f"+FULLRESYNC {repl_id} 0\r\n"))
            if input_request[4] == "?" and input_request[6] == '-1':
                binary_data = base64.b64decode(RDB_64)
                conn.send(encode(b"$" + str(len(binary_data)).encode() +b"\r\n" + binary_data))
            # conn.send("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".encode())
            replicas.add(conn)