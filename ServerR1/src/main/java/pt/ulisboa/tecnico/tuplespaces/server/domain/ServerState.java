package pt.ulisboa.tecnico.tuplespaces.server.domain;

import java.util.ArrayList;
import java.util.List;

public class ServerState {

    private List<String> tuples;


    public ServerState() {
        this.tuples = new ArrayList<String>();

    }

    public void put(String tuple) {
        tuples.add(tuple);
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
        // FIX ME BLOCKING OPERATION
        return getMatchingTuple(pattern);
    }

    public String take(String pattern) {
        // FIX ME BLOCKING OPERATION
        String tuple = getMatchingTuple(pattern);

        if (tuple != null) {
            tuples.remove(tuple);
        }

        return tuple;
    }

    public List<String> getTupleSpacesState() {
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