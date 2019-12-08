import grpc
import sys
sys.path.append('./proto')
sys.path.append('./service')
import fileService_pb2_grpc
import fileService_pb2

class ReplicationService(fileService_pb2_grpc.FileserviceServicer):

    def ReplicateFile(self, request, context):
        print("request",  str( request ) )
        return fileService_pb2.ack(success=True, message="Data Replicated.")