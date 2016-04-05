import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class JobTrackerHandler extends Thread {

    static boolean debug = true;
    List<String> dictionary;
    ZkConnector zkc;
    Socket cSocket = null;
    ObjectInputStream cin;
    ObjectOutputStream cout;
    ZooKeeper zk;
    static String ZK_JOBS = "/jobs";    // for submit tasks (jobs) only, used by worker
    static String ZK_RESULTS = "/results";

    // Setup
    public JobTrackerHandler(Socket socket, ZkConnector zkc, ZooKeeper zk) throws IOException {
        try {
            // Store variables
            this.cSocket = socket;
            this.cout = new ObjectOutputStream(cSocket.getOutputStream());
            this.cin = new ObjectInputStream(cSocket.getInputStream());
            this.zk = zk;
            this.zkc = zkc;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void debug(String s) {
        if (debug) {
            System.out.println(String.format("JT: %s", s));
        }
    }

    public void run() {
        try {
            TaskPacket p;
            while ((p = (TaskPacket) this.cin.readObject()) != null) {
                // Got a packet!
                debug("run: Retrieved packet from a worker");
                // Check if task is job or query
                if (p.packet_type == TaskPacket.SUBMIT) {
                    p = handleJob(p);
                } else if (p.packet_type == TaskPacket.STATUS) {
                    p = handleQuery(p);
                } else {
                    debug("Packet type could not be recognized");
                }
                // Send packet
                this.cout.writeObject(p);
                debug("run: Sent packet");
                // Your job is done!
                break;
            }
            this.cSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private TaskPacket handleQuery(TaskPacket p) {
        debug(String.format("handling query on '%s'", p.hash));
        String resultPath = ZK_RESULTS + "/" + p.hash;
        String response = null;
        try {
            // for query, check /result/[hash]
            Stat stat = zk.exists(resultPath, false);

            if (stat != null) {
                byte[] data = null;
                while (data == null) {
                    data = zk.getData(resultPath, false, null);
                }

                // debugging
                String dataStr = byteToString(data);
                debug("handleQuery: " + dataStr);

                String[] tokens = byteToString(data).split(":");

                //if (tokens[0].equals("0")) {
                //	response = "error: job '" + p.hash + "' could not be processed";
                if (tokens[0].equals("success")) {//} else if (tokens[0].equals("success")) {
                    response = "password found: " + tokens[1];
                } else if (tokens[0].equals("fail")) {
                    response = "password not found; try again."; //response = "error: job could not be processed";
                } else {//if (Integer.parseInt(tokens[0]) > 0) {
                    response = "job in progress";
                }
            } else {
                response = "job '" + p.hash + "' does not exist";
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new TaskPacket(TaskPacket.RESPONSE, p.hash, response);
    }

    private TaskPacket handleJob(TaskPacket p) {
        // check if task already exists in /job/[hash]
        // if not, create /job/[hash]
        debug(String.format("handling job '%s'", p.hash));
        String jobPath = ZK_JOBS + "/" + p.hash;
        String resultPath = ZK_RESULTS + "/" + p.hash;

        try {
            // check if task already exists in /job/[hash]
            Stat statJob = zk.exists(jobPath, false);
            Stat statResult = zk.exists(resultPath, false);

            // create job if it doesn't already exist
            if (statJob == null) {
                String res = zk.create(jobPath,
                        String.valueOf(1).getBytes(),  //1 node is using this job
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            if (statResult == null) {
                String res = zk.create(resultPath,
                        String.valueOf(0).getBytes(),  //init to 0
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new TaskPacket(TaskPacket.RESPONSE, p.hash, "Job submitted for " + p.hash);
    }

    public String byteToString(byte[] b) {
        String s = null;
        if (b != null) {
            try {
                s = new String(b, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return s;
    }
}
