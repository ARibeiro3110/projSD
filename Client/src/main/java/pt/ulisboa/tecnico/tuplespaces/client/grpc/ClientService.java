package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;

public class ClientService {
  
    /** Set flag to true to print debug messages. 
     * The flag can be set using the -debug command line option. */
    
    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);
    private TupleSpacesGrpc.TupleSpacesBlockingStub stub;
    private ManagedChannel channel;
    
    /** Helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }
    private final String target;
    

    public ClientService(String host, String port) {
        this.target = host + ":" + port;
        debug("Target: " + target);
        this.stub = createBlockingStub();
    }

    private TupleSpacesGrpc.TupleSpacesBlockingStub createBlockingStub() {
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TupleSpacesGrpc.newBlockingStub(channel);
    } 

    public void shutdown() {
        // Shutdown channel before stopping the process
        channel.shutdownNow();
    }
  
    public void put(String tuple) {
        try {
            stub.put(PutRequest.newBuilder().setNewTuple(tuple).build());
            System.out.println("OK");   // TODO: place outside ClientService?
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void read(String searchPattern) {
        try {
            ReadResponse result = stub.read(ReadRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult());
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void take(String searchPattern) {
        try {
            TakeResponse result = stub.take(TakeRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult());
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    // TODO: implement the remote operations


}
