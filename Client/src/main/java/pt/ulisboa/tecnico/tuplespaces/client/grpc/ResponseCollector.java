package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;

public class ResponseCollector {
    private int putResponses = 0;
    private String readTuple = "";
    private List<String> tupleSpacesState = new ArrayList<String>();

    synchronized public void incrementPutResponses() {
        putResponses++;
        notifyAll();
    }

    synchronized public void addTuple(String tuple) {
        readTuple = tuple;
        notifyAll();
    }

    synchronized public void waitForPutResponses(int numServers) throws InterruptedException {
        while (putResponses < numServers) 
            try{
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        putResponses = 0;
    }

    synchronized public void waitForReadResponse() throws InterruptedException {
        while (readTuple.equals("")){ 
            try{
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    synchronized public String getReadTuple() {
        String temp = readTuple;
        readTuple = "";
        return temp;
    }

    synchronized public void setTuples(List<String> tuples) {
        tupleSpacesState = tuples;
        notifyAll();
    }

    synchronized public List<String> getTupleSpacesStateResponse() {
        List<String> temp = tupleSpacesState;
        tupleSpacesState = new ArrayList<String>();
        return temp;
    }

    synchronized public void waitForTupleSpacesStateResponse() throws InterruptedException {
        while (tupleSpacesState.isEmpty()) 
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }
}