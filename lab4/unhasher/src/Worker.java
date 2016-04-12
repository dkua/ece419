import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public class Worker {

    static String myPath = "/workers";
    static String jobsPath = "/jobs";
    static String resultsPath = "/results";
    // ZooKeeper resources
    static ZooKeeper zk;
    static String ZK_JOBS = "/jobs";
    static String ZK_RESULTS = "/results";
    // JobTracker constants
    static boolean debug = true;
    ZkConnector zkc;
    Semaphore workerSem = new Semaphore(1);
    List<String> jobs;
    List<String> oldJobs = new ArrayList();
    static int w_id;
    static String w_id_string;

    // Start up ZooKeeper connection
    public Worker(String hosts) {
        // Try to connect to ZkConnector
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch (Exception e) {
            debug("Zookeeper connect " + e.getMessage());
        }

        zk = zkc.getZooKeeper();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            debug("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort");
            return;
        }

        Worker w = new Worker(args[0]);

        // Make your own subfolder in the Worker folder
                // Keeps count of the amount of workers currently present
                w.registerWorker();

        // Start working!
        w.start();
    }

    private static void debug(String s) {
        if (debug && w_id_string != null) {
            System.out.println(String.format("WORKER_%s: %s", w_id_string, s));
        } else {
            System.out.println(String.format("WORKER_?: %s", s));
        }
    }

    private void waitUntilExists(final String p) {
        final CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
        Stat stat = null;

        try {
            stat = zk.exists(
                    p,
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
                            boolean isMyPath = event.getPath().equals(p);
                            if (isNodeCreated && isMyPath) {
                                System.out.println(p + " created!");
                                nodeCreatedSignal.countDown();
                            }
                        }
                    });
        } catch (KeeperException e) {
            System.out.println(e.code());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Waiting for " + p + " to be created ...");

        if (stat != null) {
            debug("waitUntilExists: " + p + " is already created.");
            return;
        }

        try {
            nodeCreatedSignal.await();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        debug("waitUntilExists: " + p + " exists.");
    }

    private void registerWorker() {
        try {
            waitUntilExists(myPath);

            String path;
            path = zk.create(
                    myPath + "/",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL
            );

            w_id_string = path.split("/")[2];
            w_id = Integer.parseInt(path.split("/")[2]);
            debug("Successfuly registered with ID " + w_id);


        } catch (Exception e) {
            debug("registerWorker: Couldn't register :(");
        }
    }

    private boolean start() {
        waitUntilExists(ZK_JOBS);

        while (true) {

            jobs = new ArrayList();

            listenToPathChildren(jobsPath);

            try {
                workerSem.acquire();
            } catch (Exception e) {
                debug("Couldn't release semaphore");
            }

            List<String> newJobs;
            newJobs = getNewJobs();

            handle(newJobs);
        }
    }

    // Place a watch on the children of a given path
    private void listenToPathChildren(final String path) {
        try {
            jobs = zk.getChildren(
                    path,
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            try {
                                workerSem.release();
                            } catch (Exception e) {
                                debug("Couldn't release semaphore");
                            }
                        }

                    });

            debug("listenToPathChildren: Created a watch on " + path + " children.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> getNewJobs() {
        List<String> newJobs = new ArrayList();

        debug("getNewJobs: Traversing through jobs.");

        for (String path : jobs) {
            try {
                debug("getNewJobs: " + path);

                boolean isJobComplete;
                isJobComplete = isJobDone(path);

                if (!isJobComplete) {
                    debug("getNewJobs: Adding job " + path);
                    newJobs.add(path);
                }

            } catch (Exception e) {
                debug("getNewJobs: A path has been deleted! " + path);

                oldJobs.remove(path);
            }
        }

        for (String path : newJobs) {
            jobs.remove(path);
        }

        return newJobs;
    }

    private boolean isJobDone(String path) {
        try {
            String p = resultsPath + "/" + path;
            byte[] data = null;

            while (data == null)
                data = zk.getData(p, false, null);

            String dataStr = byteToString(data);
            dataStr = dataStr.split(":")[0];

            debug("isJobDone: " + p + " dataStr: " + dataStr);
            if (dataStr.equals("success") || dataStr.equals("fail")) {
                return true;
            }
        } catch (Exception e) {
            debug("isJobDone: Didn't work.");
        }

        return false;
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

    private void handle(List<String> newJobs) {
        for (String path : newJobs) {
            debug("handle: Sending job " + path);

            try {
                new WorkerHandler(zkc, jobsPath + "/" + path, w_id_string).start();
            } catch (Exception e) {
                debug("handle: Couldn't spawn WorkerHandler");
            }
            oldJobs.add(path);
        }
    }


}
