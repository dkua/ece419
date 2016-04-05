import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

/*

  - Keep watcher on new requests
  - When a new request appears
  - Partition work
  - Send to workers

 */

public class FileServer {

    static String myPath = "/fserver";
    static String requestsPath = "/requests";
    static String dictionaryPath;
    static ServerSocket socket;
    // ZooKeeper resources
    static Integer zkport;
    static ZooKeeper zk;  //need to lock this`
    static Lock zklock;
    static String ZK_TRACKER = "/tracker";
    static String ZK_WORKER = "/worker";
    static String ZK_FSERVER = "/fserver";
    static String ZK_REQUESTS = "/requests";
    static String ZK_RESULTS = "/results";
    // RequestTracker constants
    static String TRACKER_PRIMARY = "primary";
    static String TRACKER_BACKUP = "backup";
    static String mode;
    static boolean debug = true;
    static CountDownLatch modeSignal = new CountDownLatch(1);
    private static Integer port;
    private static String addrId;
    ZkConnector zkc;
    boolean isPrimary = false;
    Watcher watcher;
    Semaphore requestSem = new Semaphore(1);
    File dictionaryFile;
    InputStream is;
    BufferedReader br;
    List<String> requests;
    List<String> oldRequests = new ArrayList();
    List<String> dictionary;

    // Start up ZooKeeper connection
    public FileServer(String zkAddr) {
        debug("Connecting to Zookeeper");

        // Try to connect to ZkConnector
        zkc = new ZkConnector();
        try {
            zkc.connect(zkAddr);
        } catch (Exception e) {
            System.out.println("FileServer: Zookeeper connect " + e.getMessage());
        }

        zk = zkc.getZooKeeper();


        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);

            }
        };

    }

    /**
     * @param args arg0		host name and port of Zookeeper
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("ERROR: Invalid FireServer arguments!");
            return;
        }

        FileServer fs = new FileServer(args[0]);
        try {
            fs.dictionaryPath = new File(args[1]).getCanonicalPath();
            socket = new ServerSocket(0);
            fs.setPrimary();
        } catch (IOException e) {
            System.err.println("ERROR: Could not figure out dictionary path!");
            System.exit(-1);
        }

        try {
            if (!fs.isPrimary) {
                debug("main: I am the backup. Waiting to become primary");
                modeSignal.await();
            }
        } catch (Exception e) {
            debug("main: Couldn't wait on modeSignal");
        }

        // You've reached this far into the code
        // You are the primary!
        // Now, get to work.
        fs.start();
    }

    private static void debug(String s) {
        if (debug && mode != null) {
            System.out.println(String.format("FS_%s: %s", mode.toUpperCase(), s));
        } else {
            System.out.println(String.format("FS_?: %s", s));
        }
    }

    private void start() {

        try {
            debug("start: Listening for incoming connections...");
            final FileServerHandler fh;
            fh = new FileServerHandler(socket.accept(), zkc, zk, dictionary);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                   fh.shutdown();
                }
            });
            fh.start();
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }
    }

    private void getDictionary() {
        debug("getDictionary: Retrieving dictionary");

        dictionary = new ArrayList<String>();

        try {
            br = new BufferedReader(new FileReader(dictionaryPath));

            String line = null;
            int i = 0;
            // Traverse through dictionary and save it into the list
            while ((line = br.readLine()) != null) {
                //debug(line);
                dictionary.add(i, line);
                i++;
            }

            debug((i - 1) + " == " + dictionary.get(i - 1));

            debug("getDictionary: Finished retrieving dictionary");
        } catch (Exception e) {
            debug("getDictionary:Boo-hoo. Couldn't import dictionary");
            e.printStackTrace();
        }

    }

    private boolean setPrimary() {
        // Store whole dictionary as a list
        getDictionary();

        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                    myPath,         // Path of znode
                    null,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == Code.OK) {
                System.out.println("setPrimary: I'm the primary file server.");

                modeSignal.countDown();

                // Place hostname and port into that folder
                try {
                    String data = socket.getInetAddress().getHostName() + ":" + socket.getLocalPort();
                    debug("setPrimary: " + data);

                    stat = zk.setData(myPath, data.getBytes(), -1);
                } catch (Exception e) {
                    debug("setPrimary: Woops. Couldn't get hostname.");
                }

                return true;
            }
        }

        return false;
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if (path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted");
                setPrimary();
            }
        }
    }


}

