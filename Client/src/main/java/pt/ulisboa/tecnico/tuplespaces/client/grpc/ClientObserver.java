package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import io.grpc.stub.StreamObserver;
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
        } else if (r instanceof getTupleSpacesStateResponse) {
            collector.setTuples(((getTupleSpacesStateResponse) r).getTupleList());
        } else if (r instanceof TakePhase1Response) {
            collector.addTakePhase1Tuples(((TakePhase1Response) r).getReservedTuplesList());
        } else if (r instanceof TakePhase1ReleaseResponse) {
            collector.incrementTakeReleaseResponses();
        } else if (r instanceof TakePhase2Response) {
            collector.incrementTakePhase2Responses();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Received error: " + throwable);
    }

    @Override
    public void onCompleted() {}
}
