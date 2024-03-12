package pt.ulisboa.tecnico.tuplespaces.serverR2;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import pt.ulisboa.tecnico.tuplespaces.serverR2.domain.ServerUtils;
import pt.ulisboa.tecnico.tuplespaces.serverR2.grpc.ClientService;
import pt.ulisboa.tecnico.tuplespaces.serverR2.grpc.ServerStateImpl;

import java.io.IOException;

public class ServerMain {

    /** set flag to true to print debug messages.
     * the flag can be set using the -Ddebug command line option. */
    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);

    /** helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }

    public static void main(String[] args) throws IOException, InterruptedException{
        debug(ServerMain.class.getSimpleName());

        // receive and print arguments
        debug("Received " + args.length + " arguments\n");

        for (int i = 0; i < args.length; i++) {
            debug("arg[" + i + "] = " + args[i] + "\n");
        }

        // check arguments
        if (args.length < 2) {
            System.err.println("Argument(s) missing!");
            System.err.printf("Usage: mvn exec:java -Dexec.args=<port> <qualifier>", ServerMain.class.getName());
            return;
        }

        final int port = Integer.parseInt(args[0]);
        final String qualifier = args[1];
        final String target = "localhost:" + port;
        final BindableService service = new ServerStateImpl();
        final ServerUtils serverUtils = new ServerUtils(new ClientService(), target, qualifier);

        // create a new server to listen on port
        Server server = ServerBuilder.forPort(port).addService(service).build();

        // start the server
        server.start();

        // server threads are running in the background
        debug("Server started");

        // register server
        debug("Registering server...");
        serverUtils.registerServer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            debug("\nShutting down server...");
            server.shutdown();
            debug("Unregistering server...");
            serverUtils.unregisterServer();
            serverUtils.shutdown();
            debug("Server shut down.");
        }));

        // do not exit the main thread. Wait until server is terminated.
        server.awaitTermination();
    }
}
