package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.client.grpc.ResponseCollector;
import pt.ulisboa.tecnico.tuplespaces.replicaXuLiskov.contract.TupleSpacesReplicaXuLiskov.*;

public class ClientObserver<Response> implements StreamObserver<Response> {

    ResponseCollector collector;

    public ClientObserver (ResponseCollector c) {
        collector = c;
    }

    @Override
    public void onNext(Response r) {
        if (r instanceof ReadResponse) {
            collector.addTuple(((ReadResponse) r).getResult());
        } else if (r instanceof PutResponse) {
            collector.incrementPutResponses();
        }
        
        System.out.println("Received response: " + r); // TODO: remove print
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Received error: " + throwable); // TODO: remove print
    }

    @Override
    public void onCompleted() {
        System.out.println("Request completed"); // TODO: remove print
    }
}
