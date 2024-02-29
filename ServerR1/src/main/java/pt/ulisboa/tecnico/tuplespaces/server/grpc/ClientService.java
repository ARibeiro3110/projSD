package pt.ulisboa.tecnico.tuplespaces.server.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.*;

public class ClientService {

    /** Set flag to true to print debug messages.
     * The flag can be set using the -debug command line option. */
    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);

    private NameServerGrpc.NameServerBlockingStub stub;
    private ManagedChannel channel;

    /** Helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }
    private final String target;

    public ClientService(String host, int port) {
        this.target = host + ":" + port;
        debug("Target: " + target);
        this.stub = createBlockingStub();
    }

    private NameServerGrpc.NameServerBlockingStub createBlockingStub() {
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return NameServerGrpc.newBlockingStub(channel);
    }

    public void shutdown() {
        // Shutdown channel before stopping the process
        channel.shutdownNow();
    }

    public void register(String name, String qualifier, String target) {
        try {
            stub.register(RegisterRequest.newBuilder().setName(name).setQualifier(qualifier).setTarget(target).build());
            System.out.println("OK");   // TODO: place outside ClientService?
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void delete(String name, String target) {
        try {
            stub.delete(DeleteRequest.newBuilder().setName(name).setTarget(target).build());
            System.out.println("OK");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

}
