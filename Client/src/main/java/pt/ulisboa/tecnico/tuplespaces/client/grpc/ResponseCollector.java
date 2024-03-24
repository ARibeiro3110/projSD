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
    private final Condition takeResponseCondition = lock.newCondition();
    private final Condition tupleSpacesStateCondition = lock.newCondition();

    private int numServers;
    private int putResponses;
    public String readTuple;
    public String takenSearchPattern;
    private List<String> tupleSpacesState;
    private boolean expectingRead;

    private int takeResponses;

    public ResponseCollector(int numServers) {
        putResponses = 0;
        readTuple = "";
        takenSearchPattern = "";
        takeResponses = 0;
        tupleSpacesState = null;
        expectingRead = false;

        this.numServers = numServers;
    }

    public void incrementPutResponses() {
        lock.lock();
        try {
            putResponses++;
            putResponseCondition.signal();
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
                readResponseCondition.signal();
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
            tupleSpacesStateCondition.signal();
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
        } finally {
            lock.unlock();
        }
    }

    public String getTakenSearchPattern() {
        lock.lock();
        try {
            String temp = takenSearchPattern;
            takenSearchPattern = "";
            return temp;
        } finally {
            lock.unlock();
        }
    }

    public void incrementTakeResponses(String searchPattern) {
        lock.lock();
        try {
            takeResponses++;
            takenSearchPattern = searchPattern;
            takeResponseCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakeResponses() throws InterruptedException {
        lock.lock();
        try {
            while (takeResponses < numServers) {
                takeResponseCondition.await();
            }
            takeResponses = 0;
        } finally {
            lock.unlock();
        }
    }
}