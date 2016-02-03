import java.io.IOException;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

public class ClientListenerThread implements Runnable {

    private MSocket mSocket = null;
    private Hashtable<String, Client> clientTable = null;
    private ConcurrentHashMap<String, VectorClock> clockTable;

    public ClientListenerThread(MSocket mSocket, Hashtable<String, Client> clientTable, ConcurrentHashMap<String, VectorClock> clockTable) {
        this.mSocket = mSocket;
        this.clientTable = clientTable;
        this.clockTable = clockTable;
        if (Debug.debug) System.out.println("Instatiating ClientListenerThread");
    }

    public void run() {
        MPacket received = null;
        Client client = null;
        if (Debug.debug) System.out.println("Starting ClientListenerThread");
        while (true) {
            try {
                received = (MPacket) mSocket.readObject();
                System.out.println("Received " + received);
                client = clientTable.get(received.name);
                clockTable.put(received.name, received.clock);
                System.out.println("C");
                if (received.event == MPacket.UP) {
                    client.forward();
                } else if (received.event == MPacket.DOWN) {
                    client.backup();
                } else if (received.event == MPacket.LEFT) {
                    client.turnLeft();
                } else if (received.event == MPacket.RIGHT) {
                    client.turnRight();
                } else if (received.event == MPacket.FIRE) {
                    client.fire();
                } else {
                    throw new UnsupportedOperationException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
