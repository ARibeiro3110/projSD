package pt.ulisboa.tecnico.tuplespaces.serverR2.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServerState {

    private class ServerEntry {
        private String tuple;
        private boolean isLocked;
        private int clientId;
        private Lock mutex;

        public ServerEntry(String tuple, boolean isLocked, int clientId) {
            this.tuple = tuple;
            this.isLocked = isLocked;
            this.clientId = clientId;
            this.mutex = new ReentrantLock();
        }

        public String getTuple() {
            return this.tuple;
        }

        public boolean isLocked() {
            return this.isLocked;
        }

        public int getClientId() {
            return this.clientId;
        }

        private void setLocked(boolean isLocked) {
            this.isLocked = isLocked;
        }

        public void setClientId(int clientId) {
            this.clientId = clientId;
        }

        public void lock() {
            setLocked(true);
            this.mutex.lock();
        }

        public void unlock() {
            this.mutex.unlock();
            setLocked(false);
        }
    }

    private List<ServerEntry> entries;
    private static final String BGN_TUPLE = "<";
    private static final String END_TUPLE = ">";

    public ServerState() {
        this.entries = new ArrayList<ServerEntry>();
    }

    public synchronized void put(String tuple) {
        entries.add(new ServerEntry(tuple, false, 0));
        notifyAll();
    }

    private ServerEntry getFirstMatchingEntry(String pattern) {
        for (ServerEntry entry : this.entries) {
            if (entry.getTuple().matches(pattern)) {
                return entry;
            }
        }
        return null;
    }

    private List<ServerEntry> getAllMatchingEntries(String pattern) {
        List<ServerEntry> matchingEntries = new ArrayList<ServerEntry>();
        for (ServerEntry entry : this.entries) {
            if (entry.getTuple().matches(pattern)) {
                matchingEntries.add(entry);
            }
        }
        return matchingEntries;
    }

    public synchronized String read(String pattern) {
        // wait until a tuple matching the pattern is available
        while (getFirstMatchingEntry(pattern) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        return getFirstMatchingEntry(pattern).getTuple();
    }

    public synchronized List<String> takePhase1(String pattern, int clientId) {
        List<ServerEntry> matchingEntries;

        // wait until a tuple matching the pattern is available
        while ((matchingEntries = getAllMatchingEntries(pattern)).isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        List<String> tuples = new ArrayList<String>();

        // check if all matching tuples are unlocked
        for (ServerEntry entry : matchingEntries) {
            if (entry.isLocked() && entry.getClientId() != clientId)
                return null;
        }

        // lock all matching tuples
        for (ServerEntry entry : matchingEntries) {
            entry.lock();
            entry.setClientId(clientId);
            tuples.add(entry.getTuple());
        }

        return tuples;
    }

    public synchronized List<String> getTupleSpacesState() {
        List<String> tupleSpacesState = new ArrayList<String>();
        for (ServerEntry entry : this.entries) {
            tupleSpacesState.add(entry.getTuple());
        }
        return tupleSpacesState;
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