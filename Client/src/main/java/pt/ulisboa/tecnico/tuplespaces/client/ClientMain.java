package pt.ulisboa.tecnico.tuplespaces.client;

import pt.ulisboa.tecnico.tuplespaces.client.grpc.ClientService;

public class ClientMain {

    private static final String NAME_SERVER_HOST = "localhost";
    private static final int NAME_SERVER_PORT = 5001;


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
        if (args.length != 0) {
            System.err.println("Usage: mvn exec:java");
            return;
        } //FIXME maybe just delete this
        

        // create a new parser and start it
        CommandProcessor parser = new CommandProcessor(new ClientService(NAME_SERVER_HOST, NAME_SERVER_PORT));
        parser.parseInput();
    }
}
