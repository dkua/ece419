import com.sun.corba.se.spi.activation.Server;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class JobTracker extends Thread implements Watcher {

    // ZooKeeper resources
    static ZooKeeper zk;
    static ZkConnector zkc;
    static String zkAddr;
    static String ZK_TRACKER = "/tracker";
    static String ZK_WORKER = "/workers";
    static String ZK_JOBS = "/jobs";    // for submit tasks (jobs) only, used by worker
    static String ZK_RESULTS = "/results";

    // JobTracker constants
    static String myPath;
    static String TRACKER_PRIMARY = "primary";
    static String TRACKER_BACKUP = "backup";
    static String mode;
    static CountDownLatch modeSignal = new CountDownLatch(1);
    static ServerSocket socket;

    // Client tracking
    static ArrayList<String> clientList = new ArrayList<String>();
    static HashMap<String, ArrayList<String>> clientJobs = new HashMap<String, ArrayList<String>>();
    static boolean debug = true;
    static Lock debugLock = new ReentrantLock();


    public JobTracker() {
        zkc = new ZkConnector();
        try {
            socket = new ServerSocket(0);
            debug("Connecting to ZooKeeper instance zk");
            zkc.connect(zkAddr);
            zk = zkc.getZooKeeper();
            debug("Connected to ZooKeeper instance zk");
            initZNodes();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void listen() {
        boolean listening = true;
        try {
            debug("start: Listening for incoming connections...");
            while (listening) {
                new JobTrackerHandler(socket.accept(), zkc, zk).start();
            }
        } catch (IOException e) {
            System.err.println("ERROR: JobTracker could not listen on port!");
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        if (args.length != 1) {
            System.err.println("ERROR: Invalid JobTracker arguments!");
            System.exit(-1);
        }
        zkAddr = args[0];
        JobTracker jt = new JobTracker();
        if (mode == TRACKER_BACKUP) {
            debug("backup setting watch on " + ZK_TRACKER + "/" + TRACKER_PRIMARY);
            zk.exists(ZK_TRACKER + "/" + TRACKER_PRIMARY, jt);    // primary watch
            modeSignal.await();
        }
        jt.listen();
    }

    private static void debug(String s) {
        debugLock.lock();
        if (debug && mode != null) {
            System.out.println(String.format("TRACKER_%s: %s", mode.toUpperCase(), s));
        } else {
            System.out.println(String.format("TRACKER_?: %s", s));
        }
        debugLock.unlock();
    }

    private void initZNodes() {
        Stat status;
        try {
            // create /tracker, and  set self as primary or backup
            status = zk.exists(ZK_TRACKER, false);
            if (status == null) {
                zk.create(ZK_TRACKER,
                        null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);

                mode = TRACKER_PRIMARY;
                myPath = ZK_TRACKER + "/" + TRACKER_PRIMARY;
                zk.create(myPath,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                debug("Created /tracker znode and set self as primary");
            } else {
                // znode /tracker exists, check if it has a child
                // if it does, it must be the primary -> make self backup.
                // else make myself primary
                if (status.getNumChildren() == 0) {
                    mode = TRACKER_PRIMARY;
                    myPath = ZK_TRACKER + "/" + TRACKER_PRIMARY;
                    zk.create(myPath,
                            null,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL);
                    debug("in /tracker znode set self as primary");
                } else {
                    mode = TRACKER_BACKUP;
                    myPath = ZK_TRACKER + "/" + TRACKER_BACKUP;
                    zk.create(myPath,
                            null,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL);
                    debug("in /tracker znode set self as backup");
                }
            }

            // create /worker
            if (zk.exists(ZK_WORKER, false) == null) {
                zk.create(ZK_WORKER,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                debug("Created /worker znode");
            }

            // create /jobs
            if (zk.exists(ZK_JOBS, false) == null) {
                zk.create(ZK_JOBS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                debug("Created /jobs znode");
            }

            // create /results
            if (zk.exists(ZK_RESULTS, false) == null) {
                zk.create(ZK_RESULTS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                debug("Created /results znode");
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    @Override
    public void process(WatchedEvent event) {
        // JobTracker watcher: watches if primary jt fails, makes self primary
        boolean isNodeDeleted;
        try {
            isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
            String nodeName = event.getPath().split("/")[2];

            if (mode.equals(TRACKER_BACKUP) // primary failure handling
                    && isNodeDeleted
                    && nodeName.equals(TRACKER_PRIMARY)) {
                debug("detected primary failure, setting self as new primary");
                zk.delete(myPath, 0);                            // remove self as backup
                myPath = ZK_TRACKER + "/" + TRACKER_PRIMARY;    // add self as primary
                mode = TRACKER_PRIMARY;
                zk.create(myPath,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);

                modeSignal.countDown();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
