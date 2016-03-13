import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {

    private MSocket mSocket = null;
    private BlockingQueue<MPacket> eventQueue = null;
    private int LocalSequenceNumber;

    public ClientSenderThread(MSocket mSocket, BlockingQueue eventQueue) {
        this.mSocket = mSocket;
        this.eventQueue = eventQueue;
        this.LocalSequenceNumber = 0;
    }

    public void run() {
        MPacket toServer = null;
        if (Debug.debug) System.out.println("Starting ClientSenderThread");
        while (true) {
            try {
                //Take packet from queue
                toServer = (MPacket) eventQueue.take();
                toServer.sequenceNumber = this.LocalSequenceNumber;
                this.LocalSequenceNumber += 1;
                if (Debug.debug) System.out.println("Sending " + toServer);
                mSocket.writeObject(toServer);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }

        }
    }
}
