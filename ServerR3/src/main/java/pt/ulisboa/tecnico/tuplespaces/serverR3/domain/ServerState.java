package pt.ulisboa.tecnico.tuplespaces.serverR3.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServerState {
    private final Lock lock = new ReentrantLock();
    private final Condition readCondition = lock.newCondition();
    private final Condition requestCondition = lock.newCondition();
    private List<BlockedTake> blockedTakes = new ArrayList<BlockedTake>();

    private class BlockedTake {
        private final String pattern;
        private final Condition condition;
        private String tuple;

        public BlockedTake(String pattern) {
            this.pattern = pattern;
            this.condition = lock.newCondition();
            this.tuple = null;
        }

        public String getPattern() {
            return pattern;
        }

        public void await() throws InterruptedException {
            condition.await();
        }

        public void signal() {
            condition.signal();
        }

        public void setTuple(String tuple) {
            this.tuple = tuple;
        }

        public String getTuple() {
            return tuple;
        }
    }

    private List<String> tuples;
    private static final String BGN_TUPLE = "<";
    private static final String END_TUPLE = ">";
    private int next;

    public ServerState() {
        this.tuples = new ArrayList<String>();
        next = 1;
    }

    public void put(String tuple, int seqNumber) {
        lock.lock();
        try {
            while (seqNumber != next) {
                try {
                    requestCondition.await();
                } catch (InterruptedException e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }

            // check if there is a blocked take operation that matches the tuple
            for (BlockedTake blockedTake : blockedTakes) {
                if (tuple.matches(blockedTake.getPattern())) {
                    blockedTake.setTuple(tuple);
                    blockedTake.signal();
                    next++;
                    requestCondition.signalAll();
                    readCondition.signalAll();
                    return;
                }
            }

            tuples.add(tuple);
            next++;
            requestCondition.signalAll();
            readCondition.signalAll();
        } finally { 
            lock.unlock();
        }
    }

    private String getMatchingTuple(String pattern) {
        for (String tuple : this.tuples) {
            if (tuple.matches(pattern)) {
                return tuple;
            }
        }
        return null;
    }

    public String read(String pattern) {
        lock.lock();
        try {
            // wait until a tuple matching the pattern is available
            while (getMatchingTuple(pattern) == null) {
                try {
                    readCondition.await();
                } catch (InterruptedException e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }

            return getMatchingTuple(pattern);
        } finally {
            lock.unlock();
        }
    }

    public String take(String pattern, int seqNumber) {
        lock.lock();
        try {
            // wait until a tuple matching the pattern is available
            while (seqNumber != next) {
                try {
                    requestCondition.await();
                } catch (InterruptedException e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }

            String tuple;
            if ((tuple = getMatchingTuple(pattern)) != null) {
                tuples.remove(tuple);
                next++;
                requestCondition.signalAll();
                return tuple;
            }

            // wait for a put operation to add a tuple that matches the pattern

            BlockedTake blockedTake = new BlockedTake(pattern);
            blockedTakes.add(blockedTake);

            next++; // allow next operation to proceed
            requestCondition.signalAll();

            try {
                blockedTake.await();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }

            // tuple is now available

            tuple = blockedTake.getTuple();

            blockedTakes.remove(blockedTake);
            
            return tuple;
        } finally {
            lock.unlock();
        }
    }

    public List<String> getTupleSpacesState() {
        lock.lock();
        try {
            List<String> tupleSpacesState = new ArrayList<String>();
            for (String tuple : tuples) {
                tupleSpacesState.add(tuple);
            }
            return tupleSpacesState;
        } finally {
            lock.unlock();
        }
    }

    public boolean isValidInput(String input) {
        if (!input.substring(0,1).equals(BGN_TUPLE)
            ||
            !input.endsWith(END_TUPLE))
            {
            return false;
        }
        return true;
    }

}