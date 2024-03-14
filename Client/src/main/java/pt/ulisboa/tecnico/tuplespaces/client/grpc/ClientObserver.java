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
            //System.out.println("Received response: " + ((ReadResponse) r).getResult()); // TODO: remove print
        } else if (r instanceof PutResponse) {
            collector.incrementPutResponses();
            //System.out.println("Received response: put response" ); // TODO: remove print
        } else if (r instanceof getTupleSpacesStateResponse) {
            collector.setTuples(((getTupleSpacesStateResponse) r).getTupleList());
            //System.out.println("Received response: getTupleSpacesStateResponse" ); // TODO: remove print
        } else if (r instanceof TakePhase1Response) {
            //System.out.println("Received tuples list: " + ((TakePhase1Response) r).getReservedTuplesList()); // TODO: remove print
            collector.addTakePhase1Tuples(((TakePhase1Response) r).getReservedTuplesList());
        } else if (r instanceof TakePhase1ReleaseResponse) {
            collector.incrementTakeReleaseResponses();
        } else if (r instanceof TakePhase2Response) {
            collector.incrementTakePhase2Responses();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Received error: " + throwable); // TODO: remove print
    }

    @Override
    public void onCompleted() {
        //System.out.println("Request completed"); // TODO: remove print
    }
}
