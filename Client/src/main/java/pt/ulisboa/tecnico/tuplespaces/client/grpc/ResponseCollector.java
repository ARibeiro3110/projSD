package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;

public class ResponseCollector {
    private int numServers;
    private int putResponses = 0;
    private String readTuple = "";
    private List<List<String>> takeTuples;
    private List<String> tupleSpacesState;

    public ResponseCollector(int numServers) {
        takeTuples = new ArrayList<List<String>>();
        tupleSpacesState = new ArrayList<String>();

        this.numServers = numServers;

        for (int i = 0; i < numServers; i++) {
            takeTuples.add(new ArrayList<String>());
        }
    }

    synchronized public void incrementPutResponses() {
        putResponses++;
        notifyAll();
    }

    synchronized public void waitForPutResponses() throws InterruptedException {
        while (putResponses < numServers) 
            try{
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        putResponses = 0;
    }

    synchronized public String getReadTuple() {
        String temp = readTuple;
        readTuple = "";
        return temp;
    }

    synchronized public void addTuple(String tuple) {
        readTuple = tuple;
        notifyAll();
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

    synchronized public List<String> getTupleSpacesStateResponse() {
        List<String> temp = tupleSpacesState;
        tupleSpacesState = new ArrayList<String>();
        return temp;
    }

    synchronized public void setTuples(List<String> tuples) {
        tupleSpacesState = tuples;
        notifyAll();
    }

    synchronized public void waitForTupleSpacesStateResponse() throws InterruptedException {
        while (tupleSpacesState.isEmpty()) 
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    synchronized public List<String> getTakeTuplesResponse(int qualifier) {
        List<String> temp = takeTuples.get(qualifier);
        takeTuples.set(qualifier, new ArrayList<String>());
        return temp;
    }

    synchronized public void addTakeTuples(List<String> tuples) {
        takeTuples.add(tuples);
        notifyAll();
    }

    synchronized public void waitForTakeResponse() throws InterruptedException {
        while (takeTuples.size() < numServers)
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        takeTuples.clear();
    }
}