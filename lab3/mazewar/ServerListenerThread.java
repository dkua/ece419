import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

public class ServerListenerThread implements Runnable {

    private MSocket mSocket = null;
    private PriorityBlockingQueue<MPacket> pq = null;

    public ServerListenerThread(MSocket mSocket, PriorityBlockingQueue<MPacket> pq) {
        this.mSocket = mSocket;
        this.pq = pq;
    }

    public void run() {
        MPacket received = null;
        if (Debug.debug) System.out.println("Starting a listener");
        while (true) {
            try {
                received = (MPacket) mSocket.readObject();
                if (Debug.debug) System.out.println("Received: " + received);
                pq.add(received);
                if (Debug.debug) System.out.println("Enqueued: " + received);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }
}
