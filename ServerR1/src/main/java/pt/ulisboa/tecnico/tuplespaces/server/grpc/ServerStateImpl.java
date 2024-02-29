package pt.ulisboa.tecnico.tuplespaces.server.grpc;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;
import pt.ulisboa.tecnico.tuplespaces.server.domain.ServerState;

import static io.grpc.Status.*;

public class ServerStateImpl extends TupleSpacesGrpc.TupleSpacesImplBase {

    private ServerState serverState = new ServerState();

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String tuple = request.getNewTuple();

        // validate tuple
        if (!serverState.isValidTuple(tuple)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid tuple format.")
                    .asRuntimeException());
            return;
        }
        serverState.put(tuple);

        // send a single response through the stream.
        responseObserver.onNext(PutResponse.newBuilder().build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate tuple
        if (!serverState.isValidSearchPattern(searchPattern)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid search pattern format.")
                    .asRuntimeException());
            return;
        }

        String result = serverState.read(searchPattern);

        // send a single response through the stream.
        responseObserver.onNext(ReadResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    @Override
    public void take(TakeRequest request, StreamObserver<TakeResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate search pattern
        if (!serverState.isValidSearchPattern(searchPattern)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid search pattern format.")
                    .asRuntimeException());
            return;
        }

        // read tuple value
        String result = serverState.take(searchPattern);

        // send a single response through the stream
        responseObserver.onNext(TakeResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {
        // send a single response through the stream
        responseObserver.onNext(getTupleSpacesStateResponse.newBuilder().addAllTuple(serverState.getTupleSpacesState(request.getQualifier())).build());

        // notify the client that the operation has been completed
        responseObserver.onCompleted();
    }

}
