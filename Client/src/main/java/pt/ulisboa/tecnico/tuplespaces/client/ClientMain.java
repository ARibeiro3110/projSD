package pt.ulisboa.tecnico.tuplespaces.client;

import pt.ulisboa.tecnico.tuplespaces.client.grpc.ClientService;

public class ClientMain {

    static final int numServers = 3;
    public static void main(String[] args) {
        // check arguments
        if (args.length != 0) {
            System.err.println("Usage: mvn exec:java");
            return;
        }

        // create a new parser and start it
        CommandProcessor parser = new CommandProcessor(new ClientService(ClientMain.numServers));
        parser.parseInput();

        // shutdown
        System.exit(0);
    }

}
