import grpc
import sys

import cache
import json
sys.path.append('./proto')
sys.path.append('./service')
import fileService_pb2_grpc
import fileService_pb2


class ReplicationService(fileService_pb2_grpc.FileserviceServicer):

    def ReplicateFile(self, request, context):
        print("request", request)
        # next_node = request.shortest_path[request.currentpos]
        if request.currentpos == len(request.shortest_path) - 1:
            cache.saveVClock(str(request), str(request))
            return fileService_pb2.ack(success=True, message="Data Replicated.")
        else:
            # forward_server_addr = self.getneighbordata(next_node)
            forward_server_addr = self.GetNeighborfromCoordinates(request.shortest_path, request.currentpos)
            forward_port = 50051
            forward_channel = grpc.insecure_channel(forward_server_addr + ":" + str(forward_port))
            forward_stub = fileService_pb2_grpc.FileserviceStub(forward_channel)
            request.currentpos += 1
            rList = [1, 2, 3, 4, 5]
            arr = bytearray(rList)
            updated_request = fileService_pb2.FileData(initialReplicaServer=request.initialReplicaServer,
                                                       bytearray=request.bytearray, vClock=request.vClock,
                                                       shortest_path=request.shortest_path,
                                                       currentpos=request.currentpos + 1)
            forward_resp = forward_stub.ReplicateFile(updated_request)
            print("forward_resp", forward_resp)
            return fileService_pb2.ack(success=True, message="Data Forwarded.")

    def GetNeighborfromCoordinates(self, path, counter):
        path = json.loads(path)
        forward_coordinates = path[counter]
        all_neighbors = self.get_all_neighbors()
        for node in all_neighbors:
            if forward_coordinates == node.coordinates:
                return node.ipaddress