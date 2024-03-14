package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;

public class ResponseCollector {
    private int numServers;
    private int putResponses;
    public String readTuple;
    private List<String> tupleSpacesState;
    private boolean expectingRead;

    private List<List<String>> takePhase1Tuples;
    private int takeReleaseResponses;
    private int takePhase2Responses;

    public ResponseCollector(int numServers) {
        putResponses = 0;
        readTuple = "";
        takeReleaseResponses = 0;
        takePhase1Tuples = new ArrayList<List<String>>();
        takePhase2Responses = 0;
        tupleSpacesState = null;
        expectingRead = false;

        this.numServers = numServers;
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
        if (expectingRead) {
            readTuple = tuple;
            expectingRead = false;
        }
        notifyAll();
    }

    synchronized public void waitForReadResponse() throws InterruptedException {
        expectingRead = true;
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
        tupleSpacesState = null;
        return temp;
    }

    synchronized public void setTuples(List<String> tuples) {
        tupleSpacesState = tuples;
        notifyAll();
    }

    synchronized public void waitForTupleSpacesStateResponse() throws InterruptedException {
        while (tupleSpacesState == null) 
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    synchronized public List<List<String>> getTakePhase1Tuples() {
        List<List<String>> temp = takePhase1Tuples;
        takePhase1Tuples = new ArrayList<List<String>>();
        return temp;
    }

    synchronized public void addTakePhase1Tuples(List<String> tuples) {
        takePhase1Tuples.add(tuples);
        notifyAll();
    }

    synchronized public void waitForTakePhase1Response() throws InterruptedException {
        while (takePhase1Tuples.size() < numServers)
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // TODO: between wait and get, takePhase1Tuples is not reset, could that be a problem?
    }

    synchronized public void incrementTakeReleaseResponses() {
        takeReleaseResponses++;
        notifyAll();
    }

    synchronized public void waitForTakeReleaseResponse() throws InterruptedException {
        while (takeReleaseResponses < numServers)
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        takeReleaseResponses = 0;
    }

    synchronized public void incrementTakePhase2Responses() {
        takePhase2Responses++;
        notifyAll();
    }

    synchronized public void waitForTakePhase2Response() throws InterruptedException {
        while (takePhase2Responses < numServers)
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        takePhase2Responses = 0;
    }
}