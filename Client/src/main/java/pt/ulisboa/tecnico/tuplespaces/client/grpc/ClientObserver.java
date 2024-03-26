package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import io.grpc.stub.StreamObserver;
import pt.ulisboa.tecnico.tuplespaces.replicaTotalOrder.contract.TupleSpacesReplicaTotalOrder.*;


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
            collector.confirmPutResponse();
        } else if (r instanceof getTupleSpacesStateResponse) {
            collector.setTupleSpacesState(((getTupleSpacesStateResponse) r).getTupleList());
        } else if (r instanceof TakeResponse) {
            collector.setTakenTuple(((TakeResponse) r).getResult());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Received error: " + throwable);
    }

    @Override
    public void onCompleted() {}
}
