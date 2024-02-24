package pt.ulisboa.tecnico.tuplespaces.client;

import pt.ulisboa.tecnico.tuplespaces.client.grpc.ClientService;

public class ClientMain {

    /** set flag to true to print debug messages. 
     * the flag can be set using the -debug command line option. */
    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);

    /** helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }
    
    public static void main(String[] args) {

        System.out.println(ClientMain.class.getSimpleName());

        // receive and print arguments
        debug(String.format("Received %d arguments", args.length));
        for (int i = 0; i < args.length; i++) {
            debug(String.format("arg[%d] = %s", i, args[i]));
        }

        // check arguments
        if (args.length < 2) {
            System.err.println("Argument(s) missing!");
            System.err.println("Usage: mvn exec:java -Dexec.args=<host> <port>");
            return;
        }

        // get the host and the port
        final String host = args[0];
        final String port = args[1];

        // create a new parser and start it
        CommandProcessor parser = new CommandProcessor(new ClientService(host, port));
        parser.parseInput();
    }
}
