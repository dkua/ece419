import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ClientSenderThread implements Runnable {

    private MSocket mSocket = null;
    private BlockingQueue<MPacket> eventQueue = null;
    private ConcurrentHashMap<String, VectorClock> clockTable;

    public ClientSenderThread(MSocket mSocket, BlockingQueue eventQueue, ConcurrentHashMap<String, VectorClock> clockTable) {
        this.mSocket = mSocket;
        this.eventQueue = eventQueue;
        this.clockTable = clockTable;
    }

    public void run() {
        MPacket toServer = null;
        VectorClock vc = null;
        if (Debug.debug) System.out.println("Starting ClientSenderThread");
        while (true) {
            try {
                //Take packet from queue
                System.out.println("B");
                toServer = (MPacket) eventQueue.take();
                vc = this.clockTable.get(toServer.name);
                vc.increment(toServer.name);
                this.clockTable.put(toServer.name, vc);
                toServer.clock = vc;
                if (Debug.debug) System.out.println("Sending " + toServer);
                mSocket.writeObject(toServer);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }

        }
    }
}
