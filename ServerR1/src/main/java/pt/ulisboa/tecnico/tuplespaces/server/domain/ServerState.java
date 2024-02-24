package pt.ulisboa.tecnico.tuplespaces.server.domain;

import java.util.ArrayList;
import java.util.List;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesGrpc;
import pt.ulisboa.tecnico.tuplespaces.centralized.contract.TupleSpacesCentralized.*;

public class ServerState extends TupleSpacesGrpc.TupleSpacesImplBase{

  private List<String> tuples;

  public ServerState() {
    this.tuples = new ArrayList<String>();

  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
      String tuple = request.getNewTuple();

      // TODO: validate tuple

      this.tuples.add(tuple);

      // Send a single response through the stream.
      responseObserver.onNext(PutResponse.newBuilder().build());

      // Notify the client that the operation has been completed.
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

  public String take(String pattern) {
    // TODO
    return null;
  }

  public List<String> getTupleSpacesState() {
    // TODO
    return null;
  }
}
