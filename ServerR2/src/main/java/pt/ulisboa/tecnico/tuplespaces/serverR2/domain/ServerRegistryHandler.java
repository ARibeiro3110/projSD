package pt.ulisboa.tecnico.tuplespaces.serverR2.domain;

import pt.ulisboa.tecnico.tuplespaces.serverR2.grpc.ClientService;

public class ServerRegistryHandler {

    private ClientService clientService;
    private String name;
    private String target;
    private String qualifier;

    public ServerRegistryHandler(ClientService clientService, String target, String qualifier, String name) {
        this.clientService = clientService;
        this.target = target;
        this.qualifier = qualifier;
        this.name = name;
        
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
