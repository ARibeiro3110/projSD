package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupRequest;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupResponse;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;

public class ClientService {

    /** Set flag to true to print debug messages.
     * The flag can be set using the -Ddebug command line option. */

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
        this.nameServerStub = createNameServerBlockingStub(target); // create blocking stub for name server

        debug("Looking for server...");
        target = findServer(); // find tuple spaces server

        this.tupleSpacesStub = createTupleSpacesBlockingStub(target); // create blocking stub for tuple spaces server
    }

    private NameServerGrpc.NameServerBlockingStub createNameServerBlockingStub(String target) {
        debug("Creating NameServerBlockingStub for target " + target + "...");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return NameServerGrpc.newBlockingStub(channel);
    }

    private TupleSpacesGrpc.TupleSpacesBlockingStub createTupleSpacesBlockingStub(String target) {
        debug("Creating TupleSpacesBlockingStub for target " + target + "...");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TupleSpacesGrpc.newBlockingStub(channel);
    }

    public List<String> lookup(String name, String qualifier) {
        try {
            LookupResponse targets;

            if (qualifier == null) { // if no qualifier is given
                targets = nameServerStub.lookup(LookupRequest.newBuilder().setName(name).build());
            } else { // using given qualifier
                targets = nameServerStub.lookup(LookupRequest.newBuilder().setName(name).setQualifier(qualifier).build());
            }

            return targets.getTargetList(); // return list of targets

        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
            return null;
        }
    }

    private String findServer() {
        List<String> targets = lookup("TupleSpaces", null);

        while (targets == null || targets.isEmpty()) {
            try {
                System.out.println("No servers found. Retrying in 5 seconds...");
                Thread.sleep(5000); // sleep for 5 second before trying again
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }
            targets = lookup("TupleSpaces", null);
        }

        return targets.get(0);
    }

    public void put(String tuple) {
        try {
            debug("Sending put request with tuple " + tuple + "...");
            tupleSpacesStub.put(PutRequest.newBuilder().setNewTuple(tuple).build());
            System.out.println("OK\n");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void read(String searchPattern) {
        try {
            debug("Sending read request with search pattern " + searchPattern + "...");
            ReadResponse result = tupleSpacesStub.read(ReadRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult() + "\n");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void take(String searchPattern) {
        try {
            debug("Sending take request with search pattern " + searchPattern + "...");
            TakeResponse result = tupleSpacesStub.take(TakeRequest.newBuilder().setSearchPattern(searchPattern).build());
            System.out.println("OK\n" + result.getResult() + "\n");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void getTupleSpacesState(String qualifier) {
        try {
            getTupleSpacesStateResponse tuples = tupleSpacesStub.getTupleSpacesState(getTupleSpacesStateRequest.newBuilder().setQualifier(qualifier).build());
            System.out.println("OK");

            // print in format [tuple1, tuple2, ...]
            System.out.println("[" + String.join(", ", tuples.getTupleList()) + "]\n");

        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void shutdown() {
        // Shutdown channel before stopping the process
        debug("Shutting down channel...");
        channel.shutdownNow();
    }

}
