import random
import socket
from threading import Thread
import time
import os
import json


class Replicate:
    # hold infected nodes
    # initialization method.
    # pass the port of the node and the ports of the nodes connected to it
    localIP = "169.105.246.9"
    localPort = 21000
    bufferSize = 1024
    # Create a datagram socket
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind to address and ip
    UDPServerSocket.bind((localIP, localPort))
    def __init__(self):
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

    def replicateContent(self, hostname, intial_Replicate_Server):
        # logic to pick up bytes from memory and transmit
        bytes_read_from_memory = str.encode("Srinivas")

        self.transmit_message(bytes_read_from_memory, intial_Replicate_Server, hostname.decode("utf-8"), False)


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
            print("ping -c 1 " +hostname.decode("utf-8") )
            response = os.system("ping -c 1 " +hostname.decode("utf-8"))
            # and then check the response
            if response == 0:
                print(hostname, 'up')
                self.transmit_message(message, intial_Replicate_Server, hostname.decode("utf-8"), False)
                break
            else:
                print(hostname, 'down')
                ListofNeigbors.remove(forwardIP)


    def receive_message(self):
        while True:
            message, address = self.UDPServerSocket.recvfrom(1024)
            print(message)
            print(address[0])
            data = json.loads(message.decode())
            intialReplicaServer = data.get("initalReplicaServer")
            print("intialReplicaServer", intialReplicaServer)
            message = data.get("message")
            isFirstServer = data.get("isFirstServer")
            if message=="true":
                print("Trying to replicate at", address[0])
                self.replicateContent(intialReplicaServer, address[0])
            elif message.isnumeric() and isFirstServer == True:
                print("First Server", intialReplicaServer)
                self.findNeighbors(message, intialReplicaServer)
            elif message.isnumeric():
            # Logic to check for write
                canAccomodate = self.checkforCapacity(message, self.hostname)
                if canAccomodate:
                    replicate_true = str.encode("true")
                    self.transmit_message(replicate_true, intialReplicaServer, address[0].decode("utf-8"), False)
                    print("inside if")
                else:
                   self.findNeighbors(message, intialReplicaServer)
            else:
                #logic to write to memory
                print("Write to memory")



    def transmit_message(self, message, intial_Replicate_Server, hostname, firstServer):
        # loop as long as there are susceptible(connected) ports(nodes) to send to
        #data = message
        '''bytesToSend = str.encode(data)
        print(bytesToSend)'''
        # logic to chose neighbors and check if neighbor is alive and not in list of already transmitted
        serverAddressPort = ("169.105.246.4", 21000)
        bufferSize = 1024
        # Create a UDP socket at client side
        # Send to server using created UDP socket
        message = json.dumps({"isFirstServer": firstServer, "intialReplicaServer": intial_Replicate_Server, "message": message})
        print("Sending message to",message)
        self.UDPServerSocket.sendto(message.encode(), serverAddressPort)
        #time.sleep(2)

    def start_threads(self):
        Thread(target=self.receive_message).start()
