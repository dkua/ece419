import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

public class ClientListenerThread implements Runnable {

    private MSocket mSocket = null;
    private PriorityBlockingQueue<MPacket> pq = null;

    public ClientListenerThread(MSocket mSocket, PriorityBlockingQueue<MPacket> pq) {
        this.mSocket = mSocket;
        this.pq = pq;
        if (Debug.debug) System.out.println("Instantiating ClientListenerThread");
    }

    public void run() {
        MPacket received = null;
        if (Debug.debug) System.out.println("Starting ClientListenerThread");
        while (true) {
            try {
                received = (MPacket) mSocket.readObject();
                System.out.println("Received " + received);
                pq.add(received);
                System.out.println("Enqueued " + received);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
