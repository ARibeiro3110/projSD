package pt.ulisboa.tecnico.tuplespaces.server.domain;

import java.util.ArrayList;
import java.util.List;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;
import pt.ulisboa.tecnico.tuplespaces.server.grpc.ClientService;

import static io.grpc.Status.*;


public class ServerState extends TupleSpacesGrpc.TupleSpacesImplBase {

    private ClientService clientService;
    private List<String> tuples;
    private static final String name = "TupleSpaces";
    private String target;
    private String qualifier;

    public ServerState(ClientService clientService, String target, String qualifier) {
        this.clientService = clientService;
        this.target = target;
        this.qualifier = qualifier;
        this.tuples = new ArrayList<String>();
        registerServer();
    }


    private void registerServer() {
        clientService.register(name, qualifier, target); 
    }


    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String tuple = request.getNewTuple();

        // validate tuple
        if (!isValidTuple(tuple)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid tuple format.")
                    .asRuntimeException());
            return;
        }

        // add tuple to tuples list
        this.tuples.add(tuple);

        // send a single response through the stream.
        responseObserver.onNext(PutResponse.newBuilder().build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    private String getMatchingTuple(String pattern) {
        for (String tuple : this.tuples) {
        if (tuple.matches(pattern)) {
            return tuple;
        }
        }
        return null;
    }


    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate tuple
        if (!isValidSearchPattern(searchPattern)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid search pattern format.")
                    .asRuntimeException());
            return;
        }

        String result = getMatchingTuple(searchPattern);

        // if the tuple was not found
        if (result == null){
            responseObserver.onError(NOT_FOUND
                    .withDescription("Tuple not found.")
                    .asRuntimeException());
            return;
        }

        // send a single response through the stream.
        responseObserver.onNext(ReadResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }



    @Override
    public void take(TakeRequest request, StreamObserver<TakeResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate search pattern
        if (!isValidSearchPattern(searchPattern)){
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid search pattern format.")
                    .asRuntimeException());
            return;
        }

        // read tuple value
        String result = getMatchingTuple(searchPattern);

        // if the tuple was not found
        if (result == null){
            responseObserver.onError(NOT_FOUND
                    .withDescription("Tuple not found.")
                    .asRuntimeException());
            return;
        }

        // delete tuple value from tuples list
        this.tuples.remove(result);

        // send a single response through the stream
        responseObserver.onNext(TakeResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }


    @Override
    public void getTupleSpacesState(getTupleSpacesStateRequest request, StreamObserver<getTupleSpacesStateResponse> responseObserver) {
        // send a single response through the stream
        responseObserver.onNext(getTupleSpacesStateResponse.newBuilder().addAllTuple(tuples).build());

        // notify the client that the operation has been completed
        responseObserver.onCompleted();
    }


    boolean isValidTuple(String tuple) {
        // if the tuple does not match the required format, it's invalid.
        // tuple format: <string1,string2,...> with alphanumeric strings
        return tuple.matches("^<\\w+(?:,\\w+)*>$");
    }

    boolean isValidSearchPattern(String searchPattern) {
        // if the search pattern does not match the required format, it's invalid.
        // tuple format: <string1,string2,...> with any string
        return searchPattern.matches("^<[^,]+(?:,[^,]+)*>$");
    }

}
