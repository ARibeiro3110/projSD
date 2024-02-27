package pt.ulisboa.tecnico.tuplespaces.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import pt.ulisboa.tecnico.tuplespaces.server.domain.ServerState;
import pt.ulisboa.tecnico.tuplespaces.server.grpc.ClientService;

import java.io.IOException;

public class ServerMain {

    private static final String NAME_SERVER_HOST = "localhost";
    private static final int NAME_SERVER_PORT = 5001;


    public static void main(String[] args) throws IOException, InterruptedException{
        System.out.println(ServerMain.class.getSimpleName());

        // receive and print arguments
        System.out.printf("Received %d arguments%n", args.length);
            for (int i = 0; i < args.length; i++) {
                System.out.printf("arg[%d] = %s%n", i, args[i]);
        }

        // check arguments
        // TODO: qualifier is not mandatory
        if (args.length != 2) {
            System.err.println("Argument(s) missing!");
            System.err.printf("Usage: mvn exec:java -Dexec.args=<port> <qualifier>", ServerMain.class.getName());
            return;
        }

        final int port = Integer.parseInt(args[0]);
        final String qualifier = args[1];
        final String target = "localhost:" + port;
        final BindableService service = new ServerState(new ClientService(NAME_SERVER_HOST, NAME_SERVER_PORT), target, qualifier);
        

        // create a new server to listen on port
        Server server = ServerBuilder.forPort(port).addService(service).build();

        // start the server
        server.start();

        // server threads are running in the background.
        System.out.println("Server started");

        // do not exit the main thread. Wait until server is terminated.
        server.awaitTermination();
    }
}
