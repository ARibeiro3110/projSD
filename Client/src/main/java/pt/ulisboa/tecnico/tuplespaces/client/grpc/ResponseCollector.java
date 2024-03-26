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
    private boolean awaitingPutResponse;
    private boolean awaitingReadResponse;
    private boolean awaitingTakeResponse;
    private boolean awaitingStateResponse;

    public ResponseCollector() {
        readTuple = "";
        takenTuple = "";
        tupleSpacesState = null;
        awaitingPutResponse = false;
        awaitingReadResponse = false;
        awaitingTakeResponse = false;
        awaitingStateResponse = false;
    }

    public void confirmPutResponse() {
        lock.lock();
        try {
            awaitingPutResponse = false;
            putResponseCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void waitForPutResponse() throws InterruptedException {
        lock.lock();
        try {      
            awaitingPutResponse = true;
            while(awaitingPutResponse) {   
                putResponseCondition.await();
            }
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
            if (awaitingReadResponse) {
                readTuple = tuple;
                awaitingReadResponse = false;
                readResponseCondition.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitForReadResponse() throws InterruptedException {
        lock.lock();
        try {
            awaitingReadResponse = true;
            while (awaitingReadResponse) {
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
            awaitingStateResponse = false;
            tupleSpacesStateCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void waitForTupleSpacesStateResponse() throws InterruptedException {
        lock.lock();
        try {
            awaitingStateResponse = true;
            while (awaitingStateResponse) {
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
            if (awaitingTakeResponse) {
                takenTuple = tuple;
                awaitingTakeResponse = false;
                takeResponseCondition.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitForTakeResponse() throws InterruptedException {
        lock.lock();
        try {
            awaitingTakeResponse = true;
            while (awaitingTakeResponse) {
                takeResponseCondition.await();
            }
        } finally {
            lock.unlock();
        }
    }
}