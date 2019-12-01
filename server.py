from concurrent import futures
import sys
sys.path.append('./proto')
sys.path.append('./service')
from concurrent.futures import ThreadPoolExecutor
import grpc
import time
import fileService_pb2_grpc
import fileService_pb2
from ReplicationService import ReplicationService


# create a gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# use the generated function `add_CalculatorServicer_to_server`
# to add the defined class to the server
fileService_pb2_grpc.add_FileserviceServicer_to_server(ReplicationService(), server)

# listen on port 50051
print('Starting server. Listening on port 50051.')
server.add_insecure_port('[::]:50051')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)