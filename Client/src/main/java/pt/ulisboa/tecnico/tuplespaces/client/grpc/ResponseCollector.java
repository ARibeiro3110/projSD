package pt.ulisboa.tecnico.tuplespaces.client.grpc;

public class ResponseCollector {
    private int putResponses = 0;
    private String readTuple = "";

    synchronized public void incrementPutResponses() {
        putResponses++;
        notifyAll();
    }

    synchronized public void addTuple(String tuple) {
        readTuple = tuple;
        notifyAll();
    }

    synchronized public void waitForPutResponses(int numServers) throws InterruptedException {
        while (putResponses < numServers) 
            try{
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        putResponses = 0;
    }

    synchronized public void waitForReadResponse() throws InterruptedException {
        while (readTuple.equals("")){ 
            try{
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    synchronized public String getReadTuple() {
        String temp = readTuple;
        readTuple = "";
        return temp;
    }
}