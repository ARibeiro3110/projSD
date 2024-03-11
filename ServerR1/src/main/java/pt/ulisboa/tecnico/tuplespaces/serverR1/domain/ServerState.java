package pt.ulisboa.tecnico.tuplespaces.serverR1.domain;

import java.util.ArrayList;
import java.util.List;

public class ServerState {

    private List<String> tuples;
    private static final String BGN_TUPLE = "<";
    private static final String END_TUPLE = ">";

    public ServerState() {
        this.tuples = new ArrayList<String>();
    }

    public synchronized void put(String tuple) {
        tuples.add(tuple);
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

    public synchronized String take(String pattern) {
        // wait until a tuple matching the pattern is available
        while (getMatchingTuple(pattern) == null) {
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

        return tuple;
    }

    public synchronized List<String> getTupleSpacesState(String qualifier) {
        return this.tuples;
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