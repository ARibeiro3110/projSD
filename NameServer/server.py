import sys
sys.path.insert(1, '../Contract/target/generated-sources/protobuf/python')
import grpc
from concurrent import futures
import NameServer_pb2 as pb2
import NameServer_pb2_grpc as pb2_grpc

# define the port
PORT = 5001

# entry with target and qualifier of a server
class ServerEntry:
    def __init__(self, target, qualifier):
        self.target = target
        self.qualifier = qualifier
    
    def getTarget(self):
        return self.target

# entry with name of a service and list of servers that provide it
class ServiceEntry:
    def __init__(self, name):
        self.name = name
        self.serverEntries = []
    
    def addServerEntry(self, ServerEntry):
        self.serverEntries.append(ServerEntry)

    def deleteServerEntry(self, target):
        self.serverEntries = [serverEntry for serverEntry in self.serverEntries \
                                if serverEntry.target != target]


class NamingServer:
    def __init__(self):
        self.serviceEntries = {}

    def addService(self, ServiceEntry):
        self.serviceEntries[ServiceEntry.name] = ServiceEntry

    def getTargetsForService(self, name):
        return [serverEntry.getTarget() for serverEntry in self.serviceEntries[name].serverEntries]

    def getTargetsForService(self, name, qualifier):
        return self.getTargetsForService(name).filter(lambda serverEntry: serverEntry.qualifier == qualifier)

    # delete server from all services
    def deleteServer(self, target):
        for serviceEntry in self.serviceEntries.values():
            serviceEntry.deleteServerEntry(target)


class NameServerServiceImpl(pb2_grpc.NameServerServicer):
    def __init__(self):
        self.namingServer = NamingServer()

    # TODO throw exception if there are errors
    def register(self, request, context):
        print("Received register request")
        print("  Name: " + request.name)
        print("  Qualifier: " + request.qualifier)
        print("  Target: " + request.target)
        print("Sending register response")

        # add service entry if it doesn't exist
        if request.name not in self.namingServer.serviceEntries:
            self.namingServer.addService(ServiceEntry(request.name))
        
        # add server entry
        self.namingServer.serviceEntries[request.name].addServerEntry( \
            ServerEntry(request.target, request.qualifier))
        
        return pb2.RegisterResponse()
    
    def lookup(self, request, context):
        print("Received lookup request")
        print("  Name: " + request.name)
        print("  Qualifier: " + request.qualifier)
        print("Sending lookup response")

        # return list of servers with qualifier and service
        response = pb2.LookupResponse()

        if request.qualifier is None:
            targets = self.namingServer.getServiceEntry(request.name, request.qualifier)
        else:
            targets = self.namingServer.getServiceEntry(request.name)
        
        response.target.extend(targets)
        return response

    def delete(self, request, context):
        print("Received delete request")
        print("  Name: " + request.name)
        print("  Target: " + request.target)
        print("Sending delete response")

        # delete server from naming server
        self.namingServer.deleteServer(request.target)

        return pb2.DeleteResponse()


if __name__ == '__main__':
    try:
        # print received arguments
        print("Received arguments:")
        for i in range(1, len(sys.argv)):
            print("  " + sys.argv[i])

        # create server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        # add service
        pb2_grpc.add_NameServerServicer_to_server(NameServerServiceImpl(), server)
        # listen on port
        server.add_insecure_port('[::]:'+str(PORT))
        # start server
        server.start()
        # print message
        print("Server listening on port " + str(PORT))
        # print termination message
        print("Press CTRL+C to terminate")
        # wait for server to finish
        server.wait_for_termination()


    except KeyboardInterrupt:
        print("NameServer stopped")
        exit(0)
