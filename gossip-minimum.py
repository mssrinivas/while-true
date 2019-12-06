import json
import os
import random
import socket
from threading import Thread
import time
from enum import Enum
import sys
import timeit
import math
from threading import Lock, Thread


class GossipProtocol:
    capacity_of_neighbors_fixed = [1234, 3456, 7899, 7543]  # maintains the list of nodes
    totalNodes = [1234, 3456, 7899, 7543]
    sys.setrecursionlimit(200000)
    localMinimumCapacity = -sys.maxsize -1
    localIP = "169.105.246.3"
    localPort = 21000
    bufferSize = 1024
    # Create a datagram socket
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind to address and ip
    UDPServerSocket.bind((localIP, localPort))

    def __init__(self):
        self.start_threads()

    def input_message(self):
        message_to_send = "message"

    def checkforConvergence(self, message_received, blacklisted_nodes):
        if self.local_message == message_received:
            self.counter += 1
            if self.counter == 10:
                if self.IPaddress not in blacklisted_nodes:
                    blacklisted_nodes.append(self.IPaddress)
                for ip in range(len(self.listofNeighbors)):
                    if ip in blacklisted_nodes:
                        continue
                    else:
                        blacklisted_nodes.append(ip)
                if len(blacklisted_nodes) >= 0.75 * len(self.totalNodes):
                    return True
        else:
            self.counter = 1
            return False

        return False

    def updated_message_util(self, data, minimum_capacity, leastUsedIP, gossip_phase):
        # update message
        Dictionary={leastUsedIP:minimum_capacity}
        data.Dictionary = Dictionary
        data.gossip = gossip_phase
        print("Message Updated", data)
        return data

    def find_minimum_in_dictionary(self, dictionary):
        result = []
        min_value = None
        for key, value in dictionary.iteritems():
            if min_value is None or value < min_value:
                min_value = value
                result = []
            if value == min_value:
                return [key,value]

    def fetch_all_neighbors(self):
        list_of_neigbors = []
        filepath = '/tmp/neighbors.txt'
        with open(filepath, "r") as ins:
            for line in ins:
                print(line)
                list_of_neigbors.append(line)

        return list_of_neigbors


    def get_minimum_capacity_neighbors(self, initalReplicaServer):
        list_of_neigbors = []
        capacity_of_neighbors =  {}
        filepath = '/tmp/neighbors.txt'
        with open(filepath, "r") as ins:
            for line in ins:
                print(line)
                list_of_neigbors.append(line)

        while len(list_of_neigbors) > 0:
            counter=0
            forwardIP = random.choice(list_of_neigbors)
            hostname = str.encode(forwardIP)
            if hostname != initalReplicaServer:
                print("ping -c 1 " + hostname.decode("utf-8"))
                response = os.system("ping -c 1 " + hostname.decode("utf-8"))
                # and then check the response
                if response == 0:
                    print(hostname, 'up')
                    # Call to check capacity
                    capacity_of_neighbors.update[hostname] = self.capacity_of_neighbors_fixed[counter]
                    counter +=1
                    list_of_neigbors.remove(forwardIP)
                    break
                else:
                    print(hostname, 'down')
                    list_of_neigbors.remove(forwardIP)
            else:
                continue

        if len(capacity_of_neighbors) == 0:
            return [sys.maxsize, sys.maxsize]
        else:
            first_minimum = self.find_minimum_in_dictionary(capacity_of_neighbors)
            return [first_minimum[0], first_minimum[1]]

    def receive_message(self):
        while True:
            messageReceived, address = self.node.recvfrom(1024)
            data = json.loads(messageReceived.decode())
            if data.IPaddress == self.IPaddress and data.gossip == False:
                # make data.gossip == true
                list_of_neighbors = self.fetch_all_neighbors(data.IPaddress)
                minimum_capacity_neighbor = self.get_minimum_capacity_neighbors()
                max_size = sys.maxsize
                minimum_capacity = min(minimum_capacity_neighbor[1], max_size)
                self.counter = 1
                updated_message = self.updated_message_util(data, minimum_capacity, minimum_capacity_neighbor[0], True)
                for ip in range(len(list_of_neighbors)):
                    self.transmit_message(list_of_neighbors[ip], updated_message)
                time.sleep(3)
                self.replicateData()
            elif data.gossip == True and self.checkForConvergence(data) == False:
                list_of_neighbors = self.fetch_all_neighbors()
                minimum_capacity_neighbor = self.get_minimum_capacity_neighbors(data.IPaddress)
                received_minimum_capacity = list(data.Dictionary.keys())[0]
                minimum_capacity = min(minimum_capacity_neighbor[1], received_minimum_capacity)
                updated_message = self.updated_message_util(data, minimum_capacity, minimum_capacity_neighbor[0], True)
                for ip in range(len(list_of_neighbors)):
                    self.transmit_message(list_of_neighbors[ip], updated_message)

    def transmit_message(self, hostname, message_to_be_gossiped):
        serverAddressPort = (hostname, 21000)
        bufferSize = 1024
        message = json.dumps({"message": message_to_be_gossiped})
        print("Sending message to", message)
        self.UDPServerSocket.sendto(message.encode(), serverAddressPort)

    def start_threads(self):
        # Thread(target=self.replicateContent()).start()
        #Thread(target=self.retries).start()
        Thread(target=self.receive_message).start()


