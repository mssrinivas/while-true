# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import fileService_pb2 as fileService__pb2


class FileserviceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.ReplicateFile = channel.unary_unary(
        '/Fileservice/ReplicateFile',
        request_serializer=fileService__pb2.FileData.SerializeToString,
        response_deserializer=fileService__pb2.ack.FromString,
        )


class FileserviceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def ReplicateFile(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_FileserviceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'ReplicateFile': grpc.unary_unary_rpc_method_handler(
          servicer.ReplicateFile,
          request_deserializer=fileService__pb2.FileData.FromString,
          response_serializer=fileService__pb2.ack.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Fileservice', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
