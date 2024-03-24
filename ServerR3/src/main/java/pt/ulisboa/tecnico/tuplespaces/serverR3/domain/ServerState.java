package pt.ulisboa.tecnico.tuplespaces.serverR3.domain;

import java.util.ArrayList;
import java.util.List;

public class ServerState {

    private List<String> tuples;
    private static final String BGN_TUPLE = "<";
    private static final String END_TUPLE = ">";
    private int next;

    public ServerState() {
        this.tuples = new ArrayList<String>();
        next = 1;
    }

    public synchronized void put(String tuple, int seqNumber) {
        while (seqNumber != next) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
        tuples.add(tuple);
        next++;
        notifyAll();
    }

    private String getMatchingTuple(String pattern) {
        for (String tuple : this.tuples) {
            if (tuple.matches(pattern)) {
                return tuple;
            }
        }
        return null;
    }

    public synchronized String read(String pattern) {
        // wait until a tuple matching the pattern is available
        while (getMatchingTuple(pattern) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        return getMatchingTuple(pattern);
    }

    public synchronized String take(String pattern, int seqNumber) {
        // wait until a tuple matching the pattern is available
        while (seqNumber != next || getMatchingTuple(pattern) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        String tuple = getMatchingTuple(pattern);

        if (tuple != null) {
            tuples.remove(tuple);
        }

        next++;
        notifyAll();

        return tuple;
    }

    public synchronized List<String> getTupleSpacesState() {
        List<String> tupleSpacesState = new ArrayList<String>();
        for (String tuple : tuples) {
            tupleSpacesState.add(tuple);
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