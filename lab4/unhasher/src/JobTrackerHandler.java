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
    Socket socket = null;
    ObjectInputStream input;
    ObjectOutputStream output;
    ZooKeeper zk;
    static String ZK_JOBS = "/jobs";    // for submit tasks (jobs) only, used by worker
    static String ZK_RESULTS = "/results";

    // Setup
    public JobTrackerHandler(Socket socket, ZkConnector zkc, ZooKeeper zk) throws IOException {
        try {
            // Store variables
            this.socket = socket;
            this.output = new ObjectOutputStream(this.socket.getOutputStream());
            this.input = new ObjectInputStream(this.socket.getInputStream());
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
            while ((p = (TaskPacket) this.input.readObject()) != null) {
                debug("run: Retrieved packet from a worker");
                if (p.packet_type == TaskPacket.SUBMIT) {
                    p = handleJob(p);
                } else if (p.packet_type == TaskPacket.STATUS) {
                    p = handleQuery(p);
                } else {
                    debug("Packet type could not be recognized");
                }
                this.output.writeObject(p);
                debug("run: Sent packet");
                break;
            }
            this.socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private TaskPacket handleQuery(TaskPacket p) {
        debug(String.format("handling query on '%s'", p.hash));
        String resultPath = ZK_RESULTS + "/" + p.hash;
        String response = null;
        try {
            Stat stat = zk.exists(resultPath, false);

            if (stat != null) {
                byte[] data = null;
                while (data == null) {
                    data = zk.getData(resultPath, false, null);
                }

                String dataStr = byteToString(data);
                debug("handleQuery: " + dataStr);

                String[] tokens = byteToString(data).split(":");

                if (tokens[0].equals("success")) {
                    response = "password found: " + tokens[1];
                } else if (tokens[0].equals("fail")) {
                    response = "password not found; try again.";
                } else {
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
        debug(String.format("handling job '%s'", p.hash));
        String jobPath = ZK_JOBS + "/" + p.hash;
        String resultPath = ZK_RESULTS + "/" + p.hash;

        try {
            Stat statJob = zk.exists(jobPath, false);
            Stat statResult = zk.exists(resultPath, false);

            if (statJob == null) {
                String res = zk.create(jobPath,
                        String.valueOf(1).getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            if (statResult == null) {
                String res = zk.create(resultPath,
                        String.valueOf(0).getBytes(),
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
