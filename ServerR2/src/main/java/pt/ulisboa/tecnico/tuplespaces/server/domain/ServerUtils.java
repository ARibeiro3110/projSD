package pt.ulisboa.tecnico.tuplespaces.server.domain;

import pt.ulisboa.tecnico.tuplespaces.server.grpc.ClientService;

public class ServerUtils {

    private ClientService clientService;
    private static final String name = "TupleSpaces";
    private String target;
    private String qualifier;

    public ServerUtils(ClientService clientService, String target, String qualifier) {
        this.clientService = clientService;
        this.target = target;
        this.qualifier = qualifier;

    }

    public void registerServer() {
        clientService.register(name, qualifier, target);
    }

    public void unregisterServer() {
        clientService.delete(name, target);
        clientService.shutdown();
    }

    public void shutdown() {
        clientService.shutdown();
    }

}
