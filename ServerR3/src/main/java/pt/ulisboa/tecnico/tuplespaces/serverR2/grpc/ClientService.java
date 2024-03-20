package pt.ulisboa.tecnico.tuplespaces.serverR2.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.*;

public class ClientService {

    private static final String NAME_SERVER_TARGET = "localhost:5001";

    private NameServerGrpc.NameServerBlockingStub stub;
    private ManagedChannel channel;

    private final String target;

    public ClientService() {
        this.target = NAME_SERVER_TARGET;
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
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void delete(String name, String target) {
        try {
            stub.delete(DeleteRequest.newBuilder().setName(name).setTarget(target).build());
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

}
