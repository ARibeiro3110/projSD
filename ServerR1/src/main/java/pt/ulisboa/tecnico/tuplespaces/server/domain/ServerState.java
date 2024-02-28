package pt.ulisboa.tecnico.tuplespaces.server.domain;

import java.util.ArrayList;
import java.util.List;

public class ServerState {

    private List<String> tuples;


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
                // TODO: handle exception
                e.printStackTrace();
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
                // TODO: handle exception
                e.printStackTrace();
            }
        }

        String tuple = getMatchingTuple(pattern);

        if (tuple != null) {
            tuples.remove(tuple);
        }

        return tuple;
    }

    public synchronized List<String> getTupleSpacesState() {
        return this.tuples;
    }

    public boolean isValidTuple(String tuple) {
        // if the tuple does not match the required format, it's invalid.
        // tuple format: <string1,string2,...> with alphanumeric strings
        return tuple.matches("^<\\w+(?:,\\w+)*>$");
    }

    public boolean isValidSearchPattern(String searchPattern) {
        // if the search pattern does not match the required format, it's invalid.
        // tuple format: <string1,string2,...> with any string
        return searchPattern.matches("^<[^,]+(?:,[^,]+)*>$");
    }

}