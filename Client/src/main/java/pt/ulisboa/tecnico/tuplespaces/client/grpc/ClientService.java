package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import pt.ulisboa.tecnico.tuplespaces.replicaXuLiskov.contract.TupleSpacesReplicaGrpc;
import pt.ulisboa.tecnico.tuplespaces.replicaXuLiskov.contract.TupleSpacesReplicaXuLiskov.*;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupRequest;
import pt.ulisboa.tecnico.nameserver.contract.NameServerOuterClass.LookupResponse;
import pt.ulisboa.tecnico.nameserver.contract.NameServerGrpc;
import pt.ulisboa.tecnico.tuplespaces.client.util.OrderedDelayer;

public class ClientService {

    /** Default host and port for the name server. */
    private static final String NAME_SERVER_TARGET = "localhost:5001";

    /** Set flag to true to print debug messages.
     * The flag can be set using the -Ddebug command line option. */

    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);
    private NameServerGrpc.NameServerBlockingStub nameServerStub;
    private List<TupleSpacesReplicaGrpc.TupleSpacesReplicaStub> tupleSpacesStubs;
    private List<ManagedChannel> channels;
    private OrderedDelayer delayer;
    private int numServers;
    private int clientId;
    private ResponseCollector collector;

    /** Helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }

    public ClientService(int numServers, int clientId) {
        this.numServers = numServers;
        this.clientId = clientId;

        delayer = new OrderedDelayer(numServers);
        collector = new ResponseCollector(numServers);

        channels = new ArrayList<ManagedChannel>();
        tupleSpacesStubs = new ArrayList<TupleSpacesReplicaGrpc.TupleSpacesReplicaStub>();

        this.nameServerStub = createNameServerBlockingStub(); // create blocking stub for name server

        debug("Looking for servers...");
        findServers();
    }

    /* This method allows the command processor to set the request delay assigned to a given server */
    public void setDelay(int id, int delay) {
        delayer.setDelay(id, delay);

        /* TODO: Remove this debug snippet */
        System.out.println("[Debug only]: After setting the delay, I'll test it");
        for (Integer i : delayer) {
          System.out.println("[Debug only]: Now I can send request to stub[" + i + "]");
      }
      System.out.println("[Debug only]: Done.");
    }

    private NameServerGrpc.NameServerBlockingStub createNameServerBlockingStub() {
        debug("Creating NameServerBlockingStub for target " + NAME_SERVER_TARGET + "...");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(NAME_SERVER_TARGET).usePlaintext().build();
        return NameServerGrpc.newBlockingStub(channel);
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

            List<String> targets = lookup("TupleSpaces", qualifier);

            while (targets == null || targets.isEmpty()) {
                try {
                    System.out.println("Server with qualifier " + qualifier + " not found. Retrying in 5 seconds...");
                    Thread.sleep(5000); // sleep for 5 second before trying again
                } catch (InterruptedException e) {
                    System.out.println("Caught exception: " + e.getMessage());
                }
                targets = lookup("TupleSpaces", qualifier);
            }

            createTupleSpacesStub(targets.get(0));
        }
    }

    public void put(String tuple) {
        try {
            debug("Sending put request with tuple " + tuple + "...");

            for (int i = 0; i < numServers; i++) {
                PutRequest request = PutRequest.newBuilder().setNewTuple(tuple).build();
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

            for (int i = 0; i < numServers; i++) {
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

    public void takePhase1(String searchPattern) {
        try {
            debug("Sending take request (phase 1) with search pattern " + searchPattern + "...");

            while (true) {
                for (int i = 0; i < numServers; i++) {
                    TakePhase1Request request = TakePhase1Request.newBuilder().setSearchPattern(searchPattern).setClientId(clientId).build();
                    tupleSpacesStubs.get(i).takePhase1(request, new ClientObserver<TakePhase1Response>(collector));
                }

                try {
                    collector.waitForTakePhase1Response();
                } catch (InterruptedException e) {
                    System.out.println("Caught exception: " + e.getMessage());
                }

                List<List<String>> takePhase1Tuples = collector.getTakePhase1Tuples();

                // count number of rejections (null responses)
                int numRejections = (int) takePhase1Tuples.stream().filter(t -> t == null).count();

                if (numRejections == 0) {
                    // received confirmation from all servers
                    List<String> intersection = intersection(takePhase1Tuples);
                    if (!intersection.isEmpty()) {
                        // end of phase 1, send take phase 2 request
                        takePhase2(intersection.get(0), clientId);
                        break;

                    } else {
                        // no intersection, backoff
                        try {
                            Thread.sleep(1000); // TODO: decide on backoff time
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // resend take phase 1 request
                        continue;
                    }
                }

                else if (numRejections == 1) {
                    // a majority of servers accepted the request
                    // backoff and resend take phase 1 request
                    try {
                        Thread.sleep(1000); // TODO: decide on backoff time
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // resend take phase 1 request
                    continue;
                }

                else if (numRejections == 2) {
                    // only a minority of servers accepted the request
                    // send take phase 1 release request
                    takePhase1Release(clientId);

                    // backoff
                    try {
                        Thread.sleep(1000); // TODO: decide on backoff time
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // resend take phase 1 request
                    continue;
                }

                else if (numRejections == 3) {
                    // rejected by all servers
                    // resend take phase 1 request
                    Random random = new Random();
                    try {
                        Thread.sleep(random.nextInt(5000)); // TODO: decide on backoff time
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // resend take phase 1 request
                    continue;
                }

                else {
                    System.out.println("Error: unexpected number of rejections");;
                }

            }

        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void takePhase1Release(int clientId) {
        try {
            debug("Sending take phase 1 release request with client id " + clientId + "...");

            for (int i = 0; i < numServers; i++) {
                TakePhase1ReleaseRequest request = TakePhase1ReleaseRequest.newBuilder().setClientId(clientId).build();
                tupleSpacesStubs.get(i).takePhase1Release(request, new ClientObserver<TakePhase1ReleaseResponse>(collector));
            }

            try {
                collector.waitForTakeReleaseResponse();
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }

        } catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public void takePhase2(String tuple, int clientId) {
        try {
            debug("Sending take request (phase 2) with tuple " + tuple + " and client id " + clientId + "...");

            for (int i = 0; i < numServers; i++) {
                TakePhase2Request request = TakePhase2Request.newBuilder().setTuple(tuple).setClientId(clientId).build();
                tupleSpacesStubs.get(i).takePhase2(request, new ClientObserver<TakePhase2Response>(collector));
            }

            try {
                collector.waitForTakePhase2Response(); // TODO: resend remove request if not all servers respond (step 3)
                                                       // TODO: when does this happen?
            } catch (InterruptedException e) {
                System.out.println("Caught exception: " + e.getMessage());
            }
        }
        catch (StatusRuntimeException e) {
            System.out.println("Caught exception with description: " +
            e.getStatus().getDescription());
        }
    }

    public List<String> intersection(List<List<String>> lists){
        if (lists.size() == 1) // only one list
            return new ArrayList<String>(); // TODO: what should we return here?

        List<String> intersection = new ArrayList<String>(lists.get(0));
        for (int i = 1; i < lists.size(); i++) {
            intersection.retainAll(lists.get(i));
        }

        return intersection;
    }

    public void getTupleSpacesState(int qualifier) {
        try {
            tupleSpacesStubs.get(qualifier).getTupleSpacesState(
                    getTupleSpacesStateRequest.getDefaultInstance(),
                    new ClientObserver<getTupleSpacesStateResponse>(collector));

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
