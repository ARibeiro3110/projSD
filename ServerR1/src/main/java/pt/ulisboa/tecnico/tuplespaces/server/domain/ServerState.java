package pt.ulisboa.tecnico.tuplespaces.server.domain;

import java.util.ArrayList;
import java.util.List;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;

import static io.grpc.Status.INVALID_ARGUMENT;


public class ServerState extends TupleSpacesGrpc.TupleSpacesImplBase {

    private List<String> tuples;

    public ServerState() {
        this.tuples = new ArrayList<String>();
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

    public String read(String pattern) {
        return getMatchingTuple(pattern);
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        String searchPattern = request.getSearchPattern();

        // validate tuple
        if (!isValidSearchPattern(searchPattern)) {
            responseObserver.onError(INVALID_ARGUMENT
                    .withDescription("Invalid seaarch pattern format.")
                    .asRuntimeException());
            return;
        }

        String result = getMatchingTuple(searchPattern);

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

        // delete tuple value from tuples list
        this.tuples.remove(result);

        // send a single response through the stream.
        responseObserver.onNext(TakeResponse.newBuilder().setResult(result).build());

        // notify the client that the operation has been completed.
        responseObserver.onCompleted();
    }

    public List<String> getTupleSpacesState() {
        // TODO
        return null;
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
