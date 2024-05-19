import time
from dataclasses import dataclass
import base64
import threading

RDB_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

@dataclass
class Constant:
    NULL_BULK_STRING = b"$-1\r\n"
    CLRF = "\r\n"
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
    WAIT = "wait"

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
replicas = {}

set_count = 0

num_replicas_ack = 0
num_replica_ack_cond = threading.Condition() 

@dataclass
class Offset:
    offset = 0

def receive_server_properties(server_properties):
    CommandProperties.ROLE = server_properties.ROLE
    CommandProperties.HOST = server_properties.HOST
    CommandProperties.PORT = server_properties.PORT
    CommandProperties.MASTER_REPLID = server_properties.MASTER_REPLID
    CommandProperties.MASTER_REPL_OFFSET = server_properties.MASTER_REPL_OFFSET
    CommandProperties.MASTER_HOST = server_properties.MASTER_HOST
    CommandProperties.MASTER_PORT = server_properties.MASTER_PORT

def raise_error(length, size):
    if length < size:
        raise Exception("Invalid Command")

def encode(msg):
    if type(msg) is str:
        return msg.encode()
    return msg

def command_decoder(data):
    input_request = data.split(Constant.CLRF)
    if input_request[-1] == "":
        input_request.pop()
    raise_error(len(input_request), 3)
    return input_request

def is_master(conn,msg):
    if CommandProperties.ROLE == Constant.MASTER:
        return conn.send(msg)


def response_handler(data,conn):
    input_request = command_decoder(data)
    command = input_request[2]
    global num_replicas_ack
    match command.lower():
        case Command.PING:
            is_master(conn,encode("+PONG\r\n"))
        
        case Command.ECHO:
            raise_error(len(input_request), 5)
            msg = input_request[4]
            is_master(conn, encode(f"${len(msg)}\r\n{input_request[4]}\r\n"))
        
        case Command.SET:
            raise_error(len(input_request), 7)
            #set_count +=1
            key = input_request[4]
            value = input_request[6]
            data_store[key] = {"value" : value, "px": None}
            if len(input_request) > 7:
                px_time = int(input_request[10])
                data_store[key]["px"] = time.time() * 1000 + px_time
            if CommandProperties.ROLE == Constant.MASTER:
                #print("Current Replicas to send ", replicas)
                for replica in replicas.keys():
                    replica.send(data.encode())
                    
                conn.send(encode(f"+OK\r\n"))
    
        case Command.GET:
            raise_error(len(input_request), 5)
            #print("Current Role", CommandProperties.ROLE)
            #print("Current Data Store ", data_store)
            key = input_request[4]
            current_time = time.time() * 1000
            if key in data_store:
                if not data_store[key]["px"] or current_time < data_store[key]["px"]:
                    value = data_store[key]["value"]
                    conn.send(encode(f"${len(value)}\r\n{value}\r\n"))
                data_store.pop(key)
            else:
                conn.send(Constant.NULL_BULK_STRING)
        
        case Command.INFO:
            raise_error(len(input_request), 5)
            if input_request[4].lower() == Constant.REPL:
                if CommandProperties.ROLE == "master":
                    msg = f"role:{CommandProperties.ROLE}master_replid:{CommandProperties.MASTER_REPLID}master_repl_offset:{CommandProperties.MASTER_REPL_OFFSET}"
                else:
                    msg = f"role:{CommandProperties.ROLE}"
                conn.send(encode(f"${len(msg)}\r\n{msg}\r\n"))
        
        case Command.REPLCONF:
            raise_error(len(input_request), 3)
            print("INSIDE REPLCONF" , CommandProperties.ROLE)
            print("input request", input_request)
            global num_replicas_ack
            if input_request[4].lower() == Command.GETACK:
                if CommandProperties.ROLE == Constant.SLAVE:
                    conn.send(encode(f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(Offset.offset))}\r\n{Offset.offset}\r\n"))
                Offset.offset += 3
            if input_request[4] == Command.ACK:
                if CommandProperties.ROLE == Constant.MASTER:
                    #replicas[conn] = int(input_request[7])
                    print("Replica sent ACK")
                    #num_replica_ack_cond.acquire()
                    #num_replicas_ack += 1
                    #print(num_replicas_ack)
                    #num_replica_ack_cond.release()

            is_master(conn,encode(f"+OK\r\n"))
        
        case Command.PSYNC:
            raise_error(len(input_request), 7)
            repl_id = CommandProperties.MASTER_REPLID
            conn.send(encode(f"+FULLRESYNC {repl_id} 0\r\n"))
            if input_request[4] == "?" and input_request[6] == '-1':
                binary_data = base64.b64decode(RDB_64)
                conn.send(encode(b"$" + str(len(binary_data)).encode() +b"\r\n" + binary_data))
            if conn not in replicas:
                replicas[conn] = 0
        
        case Command.WAIT:
            print("OFFSET is" , Offset.offset)
            raise_error(len(input_request), 3)
            replicas_required = int(input_request[4])
            timeout = int(input_request[6])
            start_timer = time.time()
            for replica in replicas.keys():
                replica.send(b"*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
            while True:
                current_time = time.time()
                if current_time - start_timer >= timeout:
                    break
                if num_replicas_ack >= replicas_required:
                    break
            is_master(conn,encode(f":{num_replicas_ack}\r\n"))
            
    Offset.offset += len(data)
    return
