import random
import socket
from threading import Thread
import time
import os


class Replicate:
    # hold infected nodes
    # initialization method.
    # pass the port of the node and the ports of the nodes connected to it
    def __init__(self):
        self.hostname="169.105.246.3"
        self.start_threads()

    def checkforCapacity(self, message_size, hostname):
        with open('/tmp/capacity.txt', 'r') as file:
            for line in file:
                if hostname in line:
                    return 1

    def replicateContent(self, hostname):
        # logic to pick up bytes from memory and transmit
        bytes_read_from_memory = str.encode("Srinivas")
        self.transmit_message(bytes_read_from_memory,hostname)


    def findNeighbors(self, message, intial_Replicate_Server):
        ListofNeigbors = []
        filepath = '/tmp/neighbors.txt'
        with open(filepath) as fp:
            line = fp.readline()
            cnt = 1
            while line:
                line = fp.readline()
                ListofNeigbors.append(line)
                cnt += 1

        while len(ListofNeigbors) > 0:
            forwardIP = random(ListofNeigbors)
            hostname = str.encode(forwardIP)
            response = os.system("ping -c 1 " + hostname)
            # and then check the response
            if response == 0:
                print(hostname, 'up')
                self.transmit_message(message, intial_Replicate_Server, hostname)
                break
            else:
                print(hostname, 'down')
                ListofNeigbors.remove(forwardIP)


    def receive_message(self):
        while True:
            message, intial_Replicate_Server, address, forward = self.node.recvfrom(1024)
            if message=="true":
                self.replicateContent(address)
            elif message.isnumeric() and forward == True:
                self.findNeighbors(message, intial_Replicate_Server)
            elif message.isnumeric():
            # Logic to check for write
                canAccomodate = self.checkforCapacity(message, self.hostname)
                if canAccomodate:
                    replicate_true = str.encode("true")
                    self.transmit_message(replicate_true, intial_Replicate_Server)
                    print("inside if")
                else:
                   self.findNeighbors(message, intial_Replicate_Server)
            else:
                #logic to write to memory
                print("Write to memory")



    def transmit_message(self, message_size, hostname):
        # loop as long as there are susceptible(connected) ports(nodes) to send to
        #data = message
        '''bytesToSend = str.encode(data)
        print(bytesToSend)'''
        # logic to chose neighbors and check if neighbor is alive and not in list of already transmitted
        serverAddressPort = (hostname, 20001)
        bufferSize = 1024
        # Create a UDP socket at client side
        UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        # Send to server using created UDP socket
        print(serverAddressPort)
        UDPClientSocket.sendto(message_size,self.hostname, serverAddressPort)
        #time.sleep(2)

    def start_threads(self):
        Thread(target=self.receive_message).start()
