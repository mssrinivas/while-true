import sys
sys.path.append('./proto')
import grpc
import fileService_pb2
import fileService_pb2_grpc


# open a gRPC channel
channel = grpc.insecure_channel('localhost:50051')

# create a stub (client)
stub = fileService_pb2_grpc.FileserviceStub(channel)

# create a valid request message
request = fileService_pb2.FileData(initialReplicaServer="initialReplicaServer", message="message",
                                                 vClock="vClock")

# make the call
resp = stub.ReplicateFile(request)

# et voilà
print(resp)