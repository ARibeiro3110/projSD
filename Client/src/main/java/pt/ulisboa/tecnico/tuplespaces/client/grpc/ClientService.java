package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupRequest;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupResponse;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.*;

public class ClientService {

    /** Set flag to true to print debug messages. 
     * The flag can be set using the -debug command line option. */

    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);
    private NameServerGrpc.NameServerBlockingStub nameServerStub;
    private TupleSpacesGrpc.TupleSpacesBlockingStub tupleSpacesStub;
    private ManagedChannel channel;
    
    /** Helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }
    

    public ClientService(String host, int port) {
       
        String target = host + ":" + port; // target for name server
        debug("Target: " + target);
        this.nameServerStub = createNameServerBlockingStub(target); // create blocking stub for name server

        List<String> targets = lookup("TupleSpaces", "A"); // TODO: test for no qualifier

        if (targets.isEmpty()) {
            System.out.println("No servers available");
            System.exit(1); //FIXME
        }

        target = targets.get(0); // use first server available
        this.tupleSpacesStub = createTupleSpacesBlockingStub(target); // create blocking stub for tuple spaces server
    }

    private NameServerGrpc.NameServerBlockingStub createNameServerBlockingStub(String target) {
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return NameServerGrpc.newBlockingStub(channel);
    }

    private TupleSpacesGrpc.TupleSpacesBlockingStub createTupleSpacesBlockingStub(String target) {
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TupleSpacesGrpc.newBlockingStub(channel);
    }

    // should this be void or return list?
    public List<String> lookup(String name, String qualifier) { //TODO: qualifier is optional, second function?
        try {
            LookupResponse targets = nameServerStub.lookup(LookupRequest.newBuilder().setName(name).setQualifier(qualifier).build());
            return targets.getTargetList(); // return list of targets
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription()); // TODO: should we keep these prints? maybe as debugs?
            return null; //FIXME
        }
    }
  
    public void put(String tuple) {
        try {
            tupleSpacesStub.put(PutRequest.newBuilder().setNewTuple(tuple).build());
            System.out.println("OK");   // TODO: place outside ClientService?
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void read(String searchPattern) {
        try {
            ReadResponse result = tupleSpacesStub.read(ReadRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult());
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void take(String searchPattern) {
        try {
            TakeResponse result = tupleSpacesStub.take(TakeRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult());
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void getTupleSpacesState() {
        try {
            getTupleSpacesStateResponse tuples = tupleSpacesStub.getTupleSpacesState(getTupleSpacesStateRequest.newBuilder().build());
            System.out.println("OK");
            
            for (String tuple : tuples.getTupleList()){
                System.out.println(tuple);
            }
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " + 
            e.getStatus().getDescription());
        }
    }

    public void shutdown() {
        // Shutdown channel before stopping the process
        channel.shutdownNow();
    }

}
