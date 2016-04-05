import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class FileServerHandler extends Thread {

    static boolean debug = true;
    List<String> dictionary;
    ZkConnector zkc;
    Socket cSocket = null;
    ObjectInputStream cin;
    ObjectOutputStream cout;
    TaskPacket packetFromCD;
    ZooKeeper zk;
    Lock zklock;

    // Setup
    public FileServerHandler(Socket socket, ZkConnector zkc, ZooKeeper zk, List<String> dictionary) throws IOException {
        super("FileServerHandler");
        debug("FileServerHandler created.");

        try {
            // Store variables
            this.cSocket = socket;
            this.cout = new ObjectOutputStream(cSocket.getOutputStream());
            this.cin = new ObjectInputStream(cSocket.getInputStream());

            this.zk = zk;
            this.zkc = zkc;

            this.dictionary = dictionary;

            debug("Created new FileServerHandler to handle remote client");
        } catch (IOException e) {
            System.out.println("IO Exception");
        }
    }

    private static void debug(String s) {
        if (debug) {
            System.out.println(String.format("FS: %s", s));
        }
    }

    public void run() {
        // Read in packet.
        PartitionPacket packetFromWorker;

        try {
            while ((packetFromWorker = (PartitionPacket) cin.readObject()) != null) {
                // Got a packet!
                // Reply back with a partition.
                debug("run: Retrieved packet from a worker");
                PartitionPacket packetToWorker = new PartitionPacket(PartitionPacket.PARTITION_REPLY);

                int partition_id = packetFromWorker.partition_id;
                int numWorkers = packetFromWorker.numWorkers;
                debug("run: partition_id = " + partition_id + " numWorkers: " + numWorkers);

                int size = dictionary.size();
                int partitionSize = (size / numWorkers);

                // Find partition size
                int i = partitionSize * (partition_id - 1);
                int end = partitionSize * (partition_id);

                if (packetToWorker.end > (size - 1))
                    packetToWorker.end = size - 1;
                debug("run: i = " + i + " end = " + end);

                packetToWorker.size = end - i;

                // Save partition dictionary
                packetToWorker.dictionary = new ArrayList(dictionary.subList(i, end));

                // Send packet
                cout.writeObject(packetToWorker);
                debug("run: Sent packet");

                // Your job is done!
                break;
            }
        } catch (Exception e) {
            debug("Oh no! Could not work out PartitionPacket");
            e.printStackTrace();
        }

        debug("run: Exitting");
    }

    public void shutdown() {
        try {
            this.cSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
