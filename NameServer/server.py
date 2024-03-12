import sys
sys.path.insert(1, '../Contract/target/generated-sources/protobuf/python')
import grpc
from concurrent import futures
import NameServer_pb2 as pb2
import NameServer_pb2_grpc as pb2_grpc

# define the port
PORT = 5001

# debug mode
debug_mode = False
def debug(message):
    if debug_mode:
        print(message)

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
        if name not in self.serviceEntries:
            return []
        return [serverEntry.getTarget() for serverEntry in self.serviceEntries[name].getServerEntries()]

    def getTargetsForServiceQualifier(self, name, qualifier):
        if name not in self.serviceEntries:
            return []
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

    # register a server for a service
    def register(self, request, context):
        debug("Received register request")
        debug("  Name: " + request.name)
        debug("  Qualifier: " + request.qualifier)
        debug("  Target: " + request.target)

        # add service entry if it doesn't exist
        if request.name not in self.namingServer.serviceEntries:
            self.namingServer.addService(ServiceEntry(request.name))

        # add server entry
        self.namingServer.serviceEntries[request.name].addServerEntry( \
            ServerEntry(request.target, request.qualifier))

        debug("Sending register response")

        return pb2.RegisterResponse()

    # lookup server targets for a service
    def lookup(self, request, context):
        debug("Received lookup request")
        debug("  Name: " + request.name)
        if request.qualifier != "":
            debug("  Qualifier: " + request.qualifier)

        # return list of servers with qualifier and service
        response = pb2.LookupResponse()

        if request.qualifier == "":
            targets = self.namingServer.getTargetsForService(request.name)
        else:
            targets = self.namingServer.getTargetsForServiceQualifier(request.name, request.qualifier)

        # extend method adds all elements of a list to the repeated field
        response.target.extend(targets)

        debug("Sending lookup response")

        return response

    def delete(self, request, context):
        debug("Received delete request")
        debug("  Name: " + request.name)
        debug("  Target: " + request.target)

        # delete server from naming server
        self.namingServer.deleteServer(request.target)

        debug("Sending delete response")

        return pb2.DeleteResponse()


if __name__ == '__main__':
    # check if debug mode is enabled
    debug_mode = '-Ddebug' in sys.argv

    try:
        # create server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        # add service
        pb2_grpc.add_NameServerServicer_to_server(NameServerServiceImpl(), server)
        # listen on port
        server.add_insecure_port('[::]:'+str(PORT))
        # start server
        server.start()
        # debug message
        debug("Server listening on port " + str(PORT))
        # debug termination message
        debug("Press CTRL+C to terminate")
        # wait for server to finish
        server.wait_for_termination()

    except KeyboardInterrupt:
        debug("NameServer stopped")
        exit(0)
