package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.replicaTotalOrder.contract.TupleSpacesReplicaGrpc;
import pt.ulisboa.tecnico.tuplespaces.replicaTotalOrder.contract.TupleSpacesReplicaTotalOrder.*;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupRequest;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupResponse;
import pt.ulisboa.tecnico.sequencer.contract.SequencerGrpc;
import pt.ulisboa.tecnico.sequencer.contract.SequencerOuterClass.GetSeqNumberRequest;
import pt.ulisboa.tecnico.sequencer.contract.SequencerOuterClass.GetSeqNumberResponse;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;
import pt.ulisboa.tecnico.tuplespaces.client.util.OrderedDelayer;

public class ClientService {

    /** Default host and port for the name server. */
    private static final String NAME_SERVER_TARGET = "localhost:5001";
    /** Default host and port for the sequencer. */
    private static final String SEQUENCER_TARGET = "localhost:5002";

    /** Set flag to true to print debug messages.
     * The flag can be set using the -Ddebug command line option. */

    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);
    private static final String SERVICE = "TupleSpace";

    private NameServerGrpc.NameServerBlockingStub nameServerStub;
    private SequencerGrpc.SequencerBlockingStub sequencerStub;
    private List<TupleSpacesReplicaGrpc.TupleSpacesReplicaStub> tupleSpacesStubs;
    private List<ManagedChannel> channels;
    private OrderedDelayer delayer;
    private int numServers;
    private ResponseCollector collector;

    /** Helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }

    public ClientService(int numServers, int clientId) {
        this.numServers = numServers;

        delayer = new OrderedDelayer(numServers);
        collector = new ResponseCollector(numServers);

        channels = new ArrayList<ManagedChannel>();
        tupleSpacesStubs = new ArrayList<TupleSpacesReplicaGrpc.TupleSpacesReplicaStub>();

        this.nameServerStub = createNameServerBlockingStub(); // create blocking stub for name server
        this.sequencerStub = createSequencerBlockingStub(); // create blocking stub for sequencer

        debug("Looking for servers...");
        findServers();
    }

    /* This method allows the command processor to set the request delay assigned to a given server */
    public void setDelay(int id, int delay) {
        delayer.setDelay(id, delay);
    }

    private NameServerGrpc.NameServerBlockingStub createNameServerBlockingStub() {
        debug("Creating NameServerBlockingStub for target " + NAME_SERVER_TARGET + "...");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(NAME_SERVER_TARGET).usePlaintext().build();
        return NameServerGrpc.newBlockingStub(channel);
    }

    private SequencerGrpc.SequencerBlockingStub createSequencerBlockingStub() {
        debug("Creating SequencerBlockingStub for target " + SEQUENCER_TARGET + "...");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(SEQUENCER_TARGET).usePlaintext().build();
        return SequencerGrpc.newBlockingStub(channel);
    }

    private void createTupleSpacesStub(String target) {
        debug("Creating TupleSpacesBlockingStub for target " + target + "...");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        channels.add(channel);
        tupleSpacesStubs.add(TupleSpacesReplicaGrpc.newStub(channel));
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

    private void findServers() {

        List<String> qualifiers = List.of("A", "B", "C");

        for (String qualifier : qualifiers) {
            if (tupleSpacesStubs.size() == numServers)
                break;

            List<String> targets = lookup(SERVICE, qualifier);

            while (targets == null || targets.isEmpty()) {
                try {
                    System.out.println("Server with qualifier " + qualifier + " not found. Retrying in 5 seconds...");
                    Thread.sleep(5000); // sleep for 5 second before trying again
                } catch (InterruptedException e) {
                    System.out.println("Caught exception: " + e.getMessage());
                }
                targets = lookup(SERVICE, qualifier);
            }

            createTupleSpacesStub(targets.get(0));
        }
    }

    public void put(String tuple) {
        try {
            debug("Sending sequence number request to sequencer...");
            
            GetSeqNumberRequest seqRequest = GetSeqNumberRequest.newBuilder().build();
            GetSeqNumberResponse seqResponse = sequencerStub.getSeqNumber(seqRequest);

            int seqNumber = seqResponse.getSeqNumber();

            debug("Sending put request with tuple " + tuple + "...");

            for (Integer i : delayer) {
                PutRequest request = PutRequest.newBuilder().setNewTuple(tuple).setSeqNumber(seqNumber).build();
                tupleSpacesStubs.get(i).put(request, new ClientObserver<PutResponse>(collector));
            }

            try {
                collector.waitForPutResponses();
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }

            System.out.println("OK\n");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void read(String searchPattern) {
        try {
            debug("Sending read request with search pattern " + searchPattern + "...");

            for (Integer i : delayer) {
                ReadRequest request = ReadRequest.newBuilder().setSearchPattern(searchPattern).build();
                tupleSpacesStubs.get(i).read(request, new ClientObserver<ReadResponse>(collector));
            }

            try {
                collector.waitForReadResponse();
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }
            System.out.println("OK\n" + collector.getReadTuple() + "\n");
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void take(String searchPattern) {
        try {
            debug("Sending sequence number request to sequencer...");
            
            GetSeqNumberRequest seqRequest = GetSeqNumberRequest.newBuilder().build();
            GetSeqNumberResponse seqResponse = sequencerStub.getSeqNumber(seqRequest);

            int seqNumber = seqResponse.getSeqNumber();

            debug("Sending take request with search pattern " + searchPattern + "...");
            for (Integer i : delayer) {
                TakeRequest request = TakeRequest.newBuilder().setSearchPattern(searchPattern).setSeqNumber(seqNumber).build();
                tupleSpacesStubs.get(i).take(request, new ClientObserver<TakeResponse>(collector));
            }
            
            try {
                collector.waitForTakeResponses();
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }
            System.out.println("OK\n" + collector.getTakenTuple() + "\n");  // TODO: check logic of take responses. We assume that all servers return the same tuple
        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void getTupleSpacesState(int qualifier) {
        try {
            for (Integer i : delayer) {
                if (i == qualifier){
                    tupleSpacesStubs.get(qualifier).getTupleSpacesState(
                        getTupleSpacesStateRequest.getDefaultInstance(),
                        new ClientObserver<getTupleSpacesStateResponse>(collector));
                    break;
                }
            }

            try {
                collector.waitForTupleSpacesStateResponse();
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }
            List<String> tuples = collector.getTupleSpacesStateResponse();
            System.out.println("OK");

            // print in format [tuple1, tuple2, ...]
            System.out.println("[" + String.join(", ", tuples) + "]\n");

        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void shutdown() {
        // Shutdown channel before stopping the process
        debug("Shutting down channel...");
        for (ManagedChannel channel : channels)
            channel.shutdownNow();
    }

}
