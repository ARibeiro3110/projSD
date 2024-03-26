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

    public String readTuple;
    public String takenTuple;
    private List<String> tupleSpacesState;
    private boolean receivedReadResponse;
    private boolean receivedPutResponse;
    private boolean receivedTakeResponse;

    public ResponseCollector(int numServers) {
        readTuple = "";
        takenTuple = "";
        tupleSpacesState = null;
        receivedReadResponse = true;
        receivedPutResponse = true;
        receivedTakeResponse = true;
    }

    public void confirmPutResponse() {
        lock.lock();
        try {
            receivedPutResponse = true;
            putResponseCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void waitForPutResponses() throws InterruptedException {
        lock.lock();
        try {
            while (receivedPutResponse == false) {
                putResponseCondition.await();
            }
            receivedPutResponse = false;
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
            if (!receivedReadResponse) {
                readTuple = tuple;
                receivedReadResponse = true;
                readResponseCondition.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitForReadResponse() throws InterruptedException {
        lock.lock();
        try {
            receivedReadResponse = false;
            while (receivedReadResponse == false) {
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

    public void setTupleSpacesState(List<String> tuples) {
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

    public String getTakenTuple() {
        lock.lock();
        try {
            String temp = takenTuple;
            takenTuple = "";
            return temp;
        } finally {
            lock.unlock();
        }
    }

    public void setTakenTuple(String tuple) {
        lock.lock();
        try {
            if (!receivedTakeResponse) {
                receivedTakeResponse = true;
                takenTuple = tuple;
                takeResponseCondition.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakeResponses() throws InterruptedException {
        lock.lock();
        try {
            while (receivedTakeResponse == false) {
                takeResponseCondition.await();
            }
            receivedTakeResponse = false;
        } finally {
            lock.unlock();
        }
    }
}