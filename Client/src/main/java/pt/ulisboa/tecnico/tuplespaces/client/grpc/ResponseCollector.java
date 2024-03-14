package pt.ulisboa.tecnico.tuplespaces.client.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ResponseCollector {
    private final Lock lock = new ReentrantLock();
    private final Condition putResponseCondition = lock.newCondition();
    private final Condition readResponseCondition = lock.newCondition();
    private final Condition takePhase1Condition = lock.newCondition();
    private final Condition takeReleaseCondition = lock.newCondition();
    private final Condition takePhase2Condition = lock.newCondition();
    private final Condition tupleSpacesStateCondition = lock.newCondition();

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

    public void incrementPutResponses() {
        lock.lock();
        try {
            putResponses++;
            putResponseCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForPutResponses() throws InterruptedException {
        lock.lock();
        try {
            while (putResponses < numServers) {
                putResponseCondition.await();
            }
            putResponses = 0;
        } finally {
            lock.unlock();
        }
    }

    public String getReadTuple() {
        lock.lock();
        try {
            String temp = readTuple;
            readTuple = "";
            return temp;
        } finally {
            lock.unlock();
        }
    }

    public void addTuple(String tuple) {
        lock.lock();
        try {
            if (expectingRead) {
                readTuple = tuple;
                expectingRead = false;
                readResponseCondition.signalAll(); 
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitForReadResponse() throws InterruptedException {
        lock.lock();
        try {
            expectingRead = true;
            while (readTuple.equals("")) {
                readResponseCondition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public List<String> getTupleSpacesStateResponse() {
        lock.lock();
        try {
            List<String> temp = tupleSpacesState;
            tupleSpacesState = null;
            return temp;
        } finally {
            lock.unlock();
        }
    }

    public void setTuples(List<String> tuples) {
        lock.lock();
        try {
            tupleSpacesState = new ArrayList<>(tuples);
            tupleSpacesStateCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTupleSpacesStateResponse() throws InterruptedException {
        lock.lock();
        try {
            while (tupleSpacesState == null) {
                tupleSpacesStateCondition.await();
            }
            System.out.println(tupleSpacesState);
        } finally {
            lock.unlock();
        }
    }

    public List<List<String>> getTakePhase1Tuples() {
        lock.lock();
        try {
            List<List<String>> temp = takePhase1Tuples;
            takePhase1Tuples = new ArrayList<List<String>>();
            return temp;
        } finally {
            lock.unlock();
        }
    }

    public void addTakePhase1Tuples(List<String> tuples) {
        lock.lock();
        try {
            takePhase1Tuples.add(tuples);
            takePhase1Condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakePhase1Response() throws InterruptedException {
        lock.lock();
        try {
            while (takePhase1Tuples.size() < numServers) {
                takePhase1Condition.await();
            }
        } finally {
            lock.unlock();
        }
        // TODO: between wait and get, takePhase1Tuples is not reset, could that be a problem?
    }

    public void incrementTakeReleaseResponses() {
        lock.lock();
        try {
            takeReleaseResponses++;
            takeReleaseCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakeReleaseResponse() throws InterruptedException {
        lock.lock();
        try {
            while (takeReleaseResponses < numServers) {
                takeReleaseCondition.await();
            }
            takeReleaseResponses = 0;
        } finally {
            lock.unlock();
        }
    }

    public void incrementTakePhase2Responses() {
        lock.lock();
        try {
            takePhase2Responses++;
            takePhase2Condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakePhase2Response() throws InterruptedException {
        lock.lock();
        try {
            while (takePhase2Responses < numServers) {
                takePhase2Condition.await();
            }
            takePhase2Responses = 0;
        } finally {
            lock.unlock();
        }
    }
}