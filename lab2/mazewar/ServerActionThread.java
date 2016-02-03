import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class ServerActionThread implements Runnable {

    private MSocket mSocket = null;
    private BlockingQueue eventQueue = null;
    private PriorityBlockingQueue<MPacket> pq = null;
    private int LookingFor;

    public ServerActionThread(BlockingQueue eventQueue, PriorityBlockingQueue<MPacket> pq) {
        this.eventQueue = eventQueue;
        this.pq = pq;
        this.LookingFor = -1;
    }

    public void run() {
        MPacket current = null;
        if (Debug.debug) System.out.println("Starting an Actioner");
        while (true) {
            try {
                current = pq.peek();
                if (current == null) {
                    continue;
                }
                if (current.sequenceNumber != LookingFor) {
                    continue;
                } else {
                    current = pq.poll();
                    LookingFor += 1;
                }
                if (Debug.debug) System.out.println(String.format("RUNNING %s, SIZE %s", current.sequenceNumber, pq.size()));
                eventQueue.put(current);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
