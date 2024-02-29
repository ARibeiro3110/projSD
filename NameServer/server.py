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

    def getQualifier(self):
        return self.qualifier


# entry with name of a service and list of servers that provide it
class ServiceEntry:
    def __init__(self, name):
        self.name = name
        self.serverEntries = []

    def getName(self):
        return self.name

    def getServerEntries(self):
        return self.serverEntries

    def addServerEntry(self, ServerEntry):
        self.serverEntries.append(ServerEntry)

    def deleteServerEntry(self, target):
        self.serverEntries = [serverEntry for serverEntry in self.serverEntries \
                                if serverEntry.target != target]


# class that holds all service entries, each with its server entries
class NamingServer:
    def __init__(self):
        self.serviceEntries = {} # dictionary with service name as key and ServiceEntry as value

    def addService(self, ServiceEntry):
        self.serviceEntries[ServiceEntry.getName()] = ServiceEntry

    def getTargetsForService(self, name):
        return [serverEntry.getTarget() for serverEntry in self.serviceEntries[name].getServerEntries()]

    def getTargetsForServiceQualifier(self, name, qualifier):
        return [serverEntry.getTarget() for serverEntry in self.serviceEntries[name].getServerEntries() \
                if serverEntry.getQualifier() == qualifier]

    # delete server from all services
    def deleteServer(self, target):
        for serviceEntry in self.serviceEntries.values():
            serviceEntry.deleteServerEntry(target)


# implementation of the NameServer service
class NameServerServiceImpl(pb2_grpc.NameServerServicer):
    def __init__(self):
        self.namingServer = NamingServer()

    # TODO throw exception if there are errors
    # register a server for a service
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

    # lookup server targets for a service
    def lookup(self, request, context):
        print("Received lookup request")
        print("  Name: " + request.name)
        if request.qualifier != "":
            print("  Qualifier: " + request.qualifier)
        print("Sending lookup response")

        # return list of servers with qualifier and service
        response = pb2.LookupResponse()

        if request.qualifier == "":
            targets = self.namingServer.getTargetsForService(request.name)
        else:
            targets = self.namingServer.getTargetsForServiceQualifier(request.name, request.qualifier)

        # extend method adds all elements of a list to the repeated field
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
