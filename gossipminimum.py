
from threading import Lock, Thread
import math
import timeit
import json
import os
import random
import socket
from threading import Thread
import time
from enum import Enum
import sys

import grpc
import ast

sys.path.append('./proto')
sys.path.append('./service')

from proto import fileService_pb2, fileService_pb2_grpc


import timeit
import math
import cache
from threading import Lock, Thread
import collections


class GossipProtocol:
    capacity_of_neighbors_fixed = [1200, 3100, 7000, 5558]  # maintains the list of nodes
    totalNodes = [1234, 3456, 7899, 7543]
    sys.setrecursionlimit(200000)
    localMinimumCapacity = -sys.maxsize - 1
    Ipaddress = "169.105.246.3"
    localPort = 21000
    local_message = None
    bufferSize = 1024
    # Create a datagram socket
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind to address and ip
    UDPServerSocket.bind((Ipaddress, localPort))

    def __init__(self):
        self.start_threads()

    def input_message(self):
        message_to_send = "message"

    def checkforConvergence(self, data):
#        data = json.loads(data.decode())
        print("data = ", data)
        message_received = data.get("Dictionary")
        blacklisted_nodes =data.get("BlackListedNodes")
        print("MG-",message_received)
        print("BL", blacklisted_nodes)
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
            self.local_message = message_received
            self.counter = 1
            return False

        return False

    def updated_message_util(self, data, minimum_capacity, leastUsedIP, gossip_phase):
        # update message
        print("DATA = ", leastUsedIP)
        Dictionary = {leastUsedIP: minimum_capacity}
        Dict = data.get("Dictionary")
        IPaddress = data.get("IPaddress")
        gossip = data.get("gossip")
        BlackListedNodes = data.get("BlackListedNodes")
        #message = json.dumps({"IPaddress": IPaddress, "gossip": gossip_phase, "Dictionary": Dictionary, "BlackListedNodes":BlackListedNodes})
        #print("Message Updated", message)
        return IPaddress, gossip, Dictionary, BlackListedNodes

    def find_minimum_in_dictionary(self, dictionary):
        result = []
        min_value = None
        print("SICT = ", dictionary)
        mini = min(dictionary, key=lambda k: dictionary[k])
        print("MINIMUM = ",[mini, dictionary[mini]])
        return [mini, dictionary[mini]]

    def fetch_all_neighbors(self):
        list_of_neigbors = []
        filepath = 'data/neighbors.txt'
        with open(filepath, "r") as ins:
            for line in ins:
                print(line)
                line = line.strip('\n')
                list_of_neigbors.append(line)

        return list_of_neigbors

    def get_minimum_capacity_neighbors(self, initalReplicaServer):
        list_of_neigbors = []
        capacity_of_neighbors = {}
        filepath = 'data/neighbors.txt'
        with open(filepath, "r") as ins:
            for line in ins:
                print(line)
                list_of_neigbors.append(line)

        while len(list_of_neigbors) > 0:
            counter = 0
            forwardIP = random.choice(list_of_neigbors)
            hostname = str.encode(forwardIP)
            hostname2 = forwardIP
            print(" STR HOSTNAME = ", hostname)
            if hostname != initalReplicaServer:
                print("ping -c 1 " + hostname.decode("utf-8"))
                response = os.system("ping -c 1 " + hostname.decode("utf-8"))
                # and then check the response
                if response == 0:
                    print(hostname, 'up')
                    # Call to check capacity
                    if counter == 0:
                        coordinates = "(0,0)"
                    else:
                        coordinates = "(1,0)"
                    capacity_of_neighbors[hostname2] = self.getneighborcapacity(coordinates)
                    counter += 1
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
            print("C O N",capacity_of_neighbors)
            first_minimum = self.find_minimum_in_dictionary(capacity_of_neighbors)
            return [first_minimum[0], first_minimum[1]]

    def receive_message(self):
        while True:
            messageReceived, address = self.UDPServerSocket.recvfrom(1024)
            data = json.loads(messageReceived.decode())
            IPaddress = data.get("IPaddress")
            gossip_flag = data.get("gossip")
            print("xyz", IPaddress ,gossip_flag)
            Convergence_Value = self.checkforConvergence(data)
            if str(IPaddress) == "169.105.246.9" and gossip_flag == False:
                # make data.gossip == true
                list_of_neighbors = self.fetch_all_neighbors()
                minimum_capacity_neighbor = self.get_minimum_capacity_neighbors(IPaddress)
                max_size = sys.maxsize
                minimum_capacity = min(minimum_capacity_neighbor[1], max_size)
                self.counter = 1
                IPaddress, gossip, Dictionary, BlackListedNodes = self.updated_message_util(data, minimum_capacity, minimum_capacity_neighbor[0], True)
                for ip in range(len(list_of_neighbors)):
                    print("SENDING TO")
                    self.transmit_message(list_of_neighbors[ip].strip('\n'), IPaddress, gossip, Dictionary, BlackListedNodes)
                time.sleep(6)
                # self.replicateData()
               # bestnode_coordinates = self.get_best_node()
                #path =  self.bfs(self.grid,self.coordinates, bestnode_coordinates)
                #print("PATH to next replica" , path)
                #get_next_ip = self.get_next_ipaddress(path,self.coordinates)
                # make a grpc call to send data to nodes ( DATA to be written to memory, path)
                #self.replicateData()
            elif gossip_flag == True and Convergence_Value == False:
                print("IN ELIF")
                list_of_neighbors = self.fetch_all_neighbors()
                minimum_capacity_neighbor = self.get_minimum_capacity_neighbors( IPaddress)
                dict = data.get("Dictionary")
                received_minimum_capacity = list(dict.keys())[0]
                minimum_capacity = min(minimum_capacity_neighbor[1], received_minimum_capacity)
                IPaddress, gossip, Dictionary, BlackListedNodes= self.updated_message_util(data, minimum_capacity, minimum_capacity_neighbor[0], True)
                for ip in range(len(list_of_neighbors)):
                    self.transmit_message(list_of_neighbors[ip].strip('\n'), IPaddress, gossip, Dictionary, BlackListedNodes)

    def transmit_message(self, hostname, IPaddress, gossip, Dictionary, BlackListedNodes):
        serverAddressPort = (hostname, 21000)
        bufferSize = 1024
        #message = json.dumps(message_to_be_gossiped)
        #print("Sending message to", message)
        message = json.dumps({"IPaddress": IPaddress, "gossip":gossip, "Dictionary":Dictionary,"BlackListedNodes":BlackListedNodes})
        self.UDPServerSocket.sendto(message.encode(), serverAddressPort)

    #def replicateData()
        # if(self.node == initialReplciateServer)
        #   check node which has minimum memory utilization
        #   call shortestPath function(self_coordinates,set_minimum_coordinates) 
        #             returns the path list
        #   initiate a counter
        #   grpc_call(payload(file,counter,(path_list)))
        # else
        #    # unwrap the payload.counter
        #    # if(payload.counter != len(payload.pathlist)-1)
                 # payload.counter+1
                 # establish GRPC channel between nodes in the pathlist -> grpc_call(payload(file,counter,(path_list)))
                 # send data through the channel
            # else
                # replicate the file(write)
                # send acknwoledgment back using same pathlist ()

    def getneighbordata(self, next_node):
        with open('data/metadata.json', 'r') as f:
            metadata_dict = json.load(f)
        nodes = metadata_dict['nodes']
        print("all nodes", nodes[next_node])
        return nodes[next_node]

    def getneighborcapacity(self, next_node):
        with open('data/metadata.json', 'r') as f:
            metadata_dict = json.load(f)
        nodes = metadata_dict['capacities']
        print("all nodes", nodes[next_node])
        return nodes[next_node][1]

    def ReplicateFile(self, request, context):
        print("request",  request.path)
        next_node = request.shortest_path[request.currentpos]
        if request.currentpos == len( request.path ) - 1 :
            cache.set(request, request)
            return fileService_pb2.ack(success=True, message="Data Replicated.")
        else:
            forward_server_addr = self.getneighbordata(next_node)
            forward_port = 50051
            forward_channel = grpc.insecure_channel(
                forward_server_addr + ":" + str(forward_port))
            forward_stub = fileService_pb2_grpc.FileserviceStub(
                forward_channel)
            request.currentpos += 1
            forward_resp = forward_stub.ReplicateFile(request)
            print("forward_resp", forward_resp)
            return fileService_pb2.ack(success=True, message="Data Forwarded.")

    def bfs(self, grid, start, goal, rows, columns):
        queue = collections.deque([[start]])
        seen = set([start])
        while queue:
            path = queue.popleft()
            y, x = path[-1]
            if grid[y][x] == goal:
                return path
            for x2, y2 in ((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)):
                if 0 <= x2 < rows and 0 <= y2 < columns and grid[y2][x2] != 0 and (x2, y2) not in seen:
                    queue.append(path + [(x2, y2)])
                    seen.add((x2, y2))

    def start_threads(self):
        # Thread(target=self.replicateContent()).start()
        # Thread(target=self.retries).start()
        Thread(target=self.receive_message).start()
