package pt.ulisboa.tecnico.tuplespaces.client;

import pt.ulisboa.tecnico.tuplespaces.client.grpc.ClientService;

public class ClientMain {

    static final int numServers = 3;
    public static void main(String[] args) {
        // check arguments
        if (args.length != 1) {
            // must receive client id
            System.err.println("Usage: mvn exec:java -Dexec.args=\\\"<client_id>\\\"");
            return;
        }

        int clientId = Integer.parseInt(args[0]);

        // create a new parser and start it
        CommandProcessor parser = new CommandProcessor(new ClientService(ClientMain.numServers, clientId));
        parser.parseInput();

        // shutdown
        System.exit(0);
    }

}
