import java.util.Hashtable;
import java.util.concurrent.PriorityBlockingQueue;

public class ClientActionThread implements Runnable {

    private MSocket mSocket = null;
    private Hashtable<String, Client> clientTable = null;

    private Integer LastSeen = -1;
    private PriorityBlockingQueue<MPacket> pq = null;

    public ClientActionThread(MSocket mSocket, PriorityBlockingQueue<MPacket> pq, Hashtable<String, Client> clientTable) {
        this.mSocket = mSocket;
        this.clientTable = clientTable;
        this.pq = pq;
        if (Debug.debug) System.out.println("Instantiating ClientActionThread");
    }

    public void run() {
        MPacket current = null;
        Client client = null;
        if (Debug.debug) System.out.println("Starting ClientActionThread");
        while (true) {
            current = pq.peek();
            if (current == null) {
                continue;
            }
            if (Debug.debug) System.out.println(String.format("PEEKING AT %s", current.sequenceNumber));
            if (current.sequenceNumber != LastSeen + 1) {
                continue;
            } else {
                current = pq.poll();
                LastSeen += 1;
            }
            if (Debug.debug) System.out.println(String.format("RUNNING %s, SIZE %s", current.sequenceNumber, pq.size()));
            client = clientTable.get(current.name);
            if (current.event == MPacket.UP) {
                client.forward();
            } else if (current.event == MPacket.DOWN) {
                client.backup();
            } else if (current.event == MPacket.LEFT) {
                client.turnLeft();
            } else if (current.event == MPacket.RIGHT) {
                client.turnRight();
            } else if (current.event == MPacket.FIRE) {
                client.fire();
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }
}
