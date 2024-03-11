package pt.ulisboa.tecnico.tuplespaces.serverR2.grpc;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.replicaXuLiskov.contract.TupleSpacesReplicaGrpc;
import pt.ulisboa.tecnico.tuplespaces.replicaXuLiskov.contract.TupleSpacesReplicaXuLiskov.*;
import pt.ulisboa.tecnico.tuplespaces.serverR2.domain.ServerState;

import static io.grpc.Status.*;

public class ServerStateImpl extends TupleSpacesReplicaGrpc.TupleSpacesReplicaImplBase {

    /** set flag to true to print debug messages.
     * the flag can be set using the -Ddebug command line option. */
    private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);

    /** helper method to print debug messages. */
    private static void debug(String debugMessage) {
        if (DEBUG_FLAG)
            System.err.println(debugMessage);
    }

    private ServerState serverState = new ServerState();

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String tuple = request.getNewTuple();

        // validate tuple
        if (!serverState.isValidInput(tuple)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid tuple format.")
                    .asRuntimeException());
            return;
        }
        serverState.put(tuple);
        debug("Tuple " + tuple + " added to the tuple space.");

        // send a single response through the stream.
        responseObserver.onNext(PutResponse.newBuilder().build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate tuple
        if (!serverState.isValidInput(searchPattern)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid search pattern format.")
                    .asRuntimeException());
            return;
        }

        String result = serverState.read(searchPattern);
        debug("Tuple " + result + " read from the tuple space.");

        // send a single response through the stream.
        responseObserver.onNext(ReadResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    //@Override
    //public void take(TakeRequest request, StreamObserver<TakeResponse> responseObserver) {
        // String searchPattern = request.getSearchPattern();

        // // validate search pattern
        // if (!serverState.isValidInput(searchPattern)) {
        //     responseObserver.onError(INVALID_ARGUMENT
        //             .withDescription("Invalid search pattern format.")
        //             .asRuntimeException());
        //     return;
        // }

        // // read tuple value
        // String result = serverState.take(searchPattern);
        // debug("Tuple " + result + " taken from the tuple space.");

        // // send a single response through the stream
        // responseObserver.onNext(TakeResponse.newBuilder().setResult(result).build());

        // // notify the client that the operation has been completed.
        // responseObserver.onCompleted();
    //}

    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {
        // send a single response through the stream
        responseObserver.onNext(getTupleSpacesStateResponse.newBuilder().addAllTuple(serverState.getTupleSpacesState()).build());
        debug("Tuple space state sent to the client.");
        // notify the client that the operation has been completed
        responseObserver.onCompleted();
    }

}
