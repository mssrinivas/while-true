import sys
import random
import socket
from threading import Thread
import time
import os
import json
sys.path.append('./proto')
sys.path.append('./service')
from threading import Thread
import grpc
import fileService_pb2
import fileService_pb2_grpc
import cache
from multiprocessing import Queue
class Replicate:
    # hold infected nodes
    # initialization method.k8
    # pass the port of the node and the ports of the nodes connected to it
    localIP = "169.105.246.3"
    localPort = 21000
    bufferSize = 1024
    # Create a datagram socket
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind to address and ip
    UDPServerSocket.bind((localIP, localPort))

    def __init__(self):
        self.Failed_Node_Map = {}
        #self.hostname="169.105.246.3"
        #self.node = socket.socket(type=socket.SOCK_DGRAM)
        # set the address, i.e(hostname and port) of the socket
        #self.port = 22000
        # bind the address to the socket created
        #self.node.bind((self.hostname, self.port))
        self.start_threads()

    def checkforCapacity(self, message_size, hostname):
        with open('/tmp/capacity.txt', 'r') as file:
            for line in file:
                print(line)
                if hostname in line:
                    return True

    def replicateContent(self, message, initialReplicaServer, address):
        # logic to pick up bytes from memory and transmit
        bytes_read_from_memory = str.encode("Srinivas")
        intial_Replicate_Server = self.localIP
        hostname = address
        serverAddress = "localhost"
        serverPort = 50051
        channel = grpc.insecure_channel(serverAddress + ":" + str(serverPort))
        replicate_stub = fileService_pb2_grpc.FileserviceStub(channel)
        if message.countOfReplica == 1:
            vClock = cache.getFileVclock("file1")
            vClock.ip2.address = hostname
            # vClock = {
            # "ip1": {"address": self.localIP, "timestamp": time.time()},
            # "ip2": {"address": hostname, "timestamp": ""},
            # "ip3": {"address": "", "timestamp": ""}
            # }
        else:
            vClock = cache.getFileVclock("file1")
            vClock.ip3.address = hostname
            file = "file1"
            message = json.dump({"vClock":vClock, "filename":file},)
            self.transmit_message(message, intial_Replicate_Server, vClock.ip2.address.decode("utf-8"), False, 0, "sync", "file1")
            self.transmit_message(message, intial_Replicate_Server, vClock.ip3.address.decode("utf-8"), False, 0, "sync", "file1")

        #print("vclock from redis", cache.getFileVclock("file1"))
        request = fileService_pb2.FileData(initialReplicaServer=intial_Replicate_Server, bytearray=bytes_read_from_memory,
                                           vClock=json.dumps(vClock))
        resp = replicate_stub.ReplicateFile(request)
        print(resp)
        #self.transmit_message(bytes_read_from_memory, intial_Replicate_Server, hostname.decode("utf-8"), False)

    def ReplicateFile(self, request, context):
        self.write_to_mem ( request )
        print("request", request.initialReplicaServer )
        return fileService_pb2.ack(success=True, message="Data Replicated.")


    def findNeighbors(self, message, intial_Replicate_Server):
        ListofNeigbors = []
        filepath = '/tmp/neighbors.txt'
        with open(filepath, "r") as ins:
            for line in ins:
                print(line)
                ListofNeigbors.append(line)

        while len(ListofNeigbors) > 0:
            forwardIP = random.choice(ListofNeigbors)
            hostname = str.encode(forwardIP)
            if hostname != intial_Replicate_Server:
                print("ping -c 1 " +hostname.decode("utf-8") )
                response = os.system("ping -c 1 " +hostname.decode("utf-8"))
                # and then check the response
                if response == 0:
                    print(hostname, 'up')
                    self.transmit_message(message, intial_Replicate_Server, hostname.decode("utf-8"), False, 2, "write", "file1")
                    break
                else:
                    print(hostname, 'down')
                    ListofNeigbors.remove(forwardIP)
            else:
                continue


    def receive_message(self):
        while True:
            message, address = self.UDPServerSocket.recvfrom(1024)
            print(message)
            print(address[0])
            data = json.loads(message.decode())
            initialReplicaServer = data.get("initialReplicaServer")
            print("initialReplicaServer", initialReplicaServer)
            message = data.get("message")
            type = data.get("type")
            isFirstServer = data.get("isFirstServer")
            if type == "sync":
                self.sync_vector_clocks(message)
            elif message == "true":
                print("Trying to replicate at", address[0])
                self.replicateContent(message, initialReplicaServer, address[0])
            elif message.isnumeric() and isFirstServer == True:
                print("First Server", initialReplicaServer)
                self.findNeighbors(message, initialReplicaServer)
            elif message.isnumeric() and message.countOfReplica > 0:
            # # Logic to check for write
                 canAccomodate = self.checkforCapacity(message, self.localIP)
            #     if canAccomodate:
            #         replicate_true = str.encode("true")
            #         print(address[0])
            #         string = str(address[0])
            #         self.transmit_message(replicate_true, initialReplicaServer, string.decode("utf-8"), False, message.countOfReplica-1,"write", "file1")
            #         print("inside if")
            #     else:
                
                #self.findNeighbors(message, initialReplicaServer)
                # Vclock = {ip1:{address, timestamp},ip2:{address, timestamp},ip3:{address, timestamp}}
                # fileName:{{intialReplicaServer, firstServer, Bytearray, {ip1:{address, timestamp},ip2:{address, timestamp},ip3:{address, timestamp}}}
                # message : {intialReplicaServer, firstServer, Bytearray, Vclock}
            elif type=="update":
                self.write_to_mem(message)


#{message, VClock.!self.Ip, update}
#{message, VClock.!self.IP. update}

    def sync_vector_clocks(self, message):
        localmessage = cache.getFileVclock(message.filename)
        localmessage.vClock.ip2.address = message.vClock.ip2.address
        localmessage.vClockip3.address = message.vClock.ip3.address
        cache.set(message.filename, localmessage)


    def write_to_mem(self, message):
        if message.countOfReplica == 1:
            message.vClock.ip2.address = self.localIP
        else:
            message.vClock.ip3.address = self.localIP
        cache.set(message.filename, message)


    def update_to_mem(self, message):
        # logic to write to memory
        print("Write to memory")
        localMax = 0
        syncMax = 0
        a = [1, 2, 3]
        if ( cache.keyExists( "file1") ):
            localFileData = cache.getFileVclock("file1")
            localvClock = localFileData.vClock
            localtimestamp = ""
            synctimestamp = ""
            for i in range(len(a)):
                ip = ip + "i"
                if localvClock.ip.timestamp != "" and message.vClock.ip.timestamp != "":
                    localtimestamp = localvClock.ip.timestamp
                    synctimestamp = message.vClock.ip.timestamp
                elif localvClock.ip.timestamp == "" and message.vClock.ip.timestamp != "":
                    localtimestamp = "0"
                    synctimestamp = message.vClock.ip.timestamp
                elif localvClock.ip.timestamp != "" and message.vClock.ip.timestamp == "":
                    localtimestamp = localvClock.ip.timestamp
                    synctimestamp = "0"
                elif localvClock.ip.timestamp == "" and message.vClock.ip.timestamp == "":
                    continue
                if float(localtimestamp) > float(synctimestamp):
                    localMax = max(localMax, float(localtimestamp))
                elif float(localtimestamp) < float(synctimestamp):
                    syncMax = max(syncMax, float(synctimestamp))

            updatedVclock = {}
            for address in localFileData.vClock:
                updatedVclock.address.timestamp = max(address.timestamp, message.vClock.address.timestamp)
                updatedVclock.address.address = address

            if localMax != 0 and syncMax != 0:
                # conflict
                if localMax < syncMax:
                    message.vClock = updatedVclock
                    cache.set(message.filename, message)
                elif localMax > syncMax:
                    localFileData.vClock = updatedVclock
                    cache.set(message.filename, localFileData)
            elif syncMax > localMax:
                cache.set(message.filename, message)


    # broadcast updates and failed retries
    def broadcast_update(self,message):
        localFileData = cache.getFileVclock("file1")
        localvClock = localFileData.vClock
        for address in localvClock:
            if address.address != self.localIP:
                hostname = str.encode(address.address)
                print("ping -c 1 " + hostname.decode("utf-8"))
                response = os.system("ping -c 1 " + hostname.decode("utf-8"))
                # and then check the response
                if response == 0:
                    print(hostname, 'up')
                    # send update
                    #self.replicateContent()
                    # self.transmit_message(message, intial_Replicate_Server, hostname.decode("utf-8"), False)
                    break
                else:
                    # put in failed map
                    print(hostname, 'down')
                    if hostname not in self.Failed_Node_Map:
                        self.Failed_Node_Map[hostname] = Queue()
                    self.Failed_Node_Map[hostname].put(message)


    def retries(self):
        print("retrying")
        while True:
            for ips in self.Failed_Node_Map:
                hostname = str.encode(ips)
                print("ping -c 1 " + hostname.decode("utf-8"))
                response = os.system("ping -c 1 " + hostname.decode("utf-8"))
                # and then check the response
                if response == 0:
                    print(hostname, 'up')
                    q = self.Failed_Node_Map[ips]
                    while not q.empty():
                        try:
                            item = q.get()
                            #self.replicateContent()
                        except Exception as e:
                            q.put(item)
                    # send update
                    self.Failed_Node_Map.pop(ips)
                    # self.transmit_message(message, intial_Replicate_Server, hostname.decode("utf-8"), False)
                    break
                else:
                    # put in failed map
                    print(hostname, 'down')


    def transmit_message(self, message, intial_Replicate_Server, hostname, firstServer, countOfReplica, type, filename):
        # loop as long as there are susceptible(connected) ports(nodes) to send to
        #data = message
        '''bytesToSend = str.encode(data)
        print(bytesToSend)'''
        # logic to chose neighbors and check if neighbor is alive and not in list of already transmitted
        serverAddressPort = (hostname, 21000)
        bufferSize = 1024
        # Create a UDP socket at client side
        # Send to server using created UDP socket
        message = json.dumps({"isFirstServer": firstServer, "initialReplicaServer": intial_Replicate_Server, "message": message, "countofReplica": countOfReplica, "type":type, "filename":filename})
        print("Sending message to",message)
        self.UDPServerSocket.sendto(message.encode(), serverAddressPort)
        #time.sleep(2)


    def start_threads(self):
        # Thread(target=self.replicateContent()).start()
        #Thread(target=self.retries).start()
        Thread(target=self.receive_message).start()
