import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;


public class ClientDriver {
    ZooKeeper zk;
    ZkConnector zkc;
    Watcher watcher;
    static String cmd;
    static String hash;
    static String result;

    Socket socket;
    String hostname;
    int port;
    ObjectInputStream input;
    ObjectOutputStream output;

    static boolean debug = true;
    static String PRIMARY = "/tracker/primary";

    public ClientDriver(String address) {
        // Connect to ZooKeeper
        this.zkc = new ZkConnector();
        try {
            debug("Connecting to ZooKeeper instance at " + address);
            this.zkc.connect(address);
            this.zk = this.zkc.getZooKeeper();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            }
        };
        while (result == null) {
            try{ Thread.sleep(1000); } catch (Exception e) {}
            result = sendTask();
        }
        System.out.println(result);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            String err = "ERROR: Invalid ClientDriver arguments:";
            for (String s : args) {
                err = err + " " + s;
            }
            System.err.println(err);
            System.exit(-1);
        }
        if (args[1].equals("job") || args[1].equals("status")) {
            cmd = args[1];
            hash = args[2];
        } else {
            System.err.println("ERROR: Invalid command type: " + args[1]);
        }
        ClientDriver cd = new ClientDriver(args[0]);
    }

    private static void debug(String s) {
        if (debug) {
            System.out.println(String.format("CLIENT: %s", s));
        }
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(PRIMARY)) {
            if (type == EventType.NodeDeleted) {
                debug(PRIMARY + " deleted!");
                disconnect();
            }
            if (type == EventType.NodeCreated) {
                debug(PRIMARY + "created!");
                byte[] data = null;
                while (data == null) {
                    try {
                        data = this.zk.getData(PRIMARY, false, null);
                        String dataStr = byteToString(data);
                        this.hostname = dataStr.split(":")[0];
                        this.port = Integer.parseInt(dataStr.split(":")[1]);
                        connect();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void connect() {
        try {
            this.socket = new Socket(this.hostname, this.port);
            debug("Connected to " + this.hostname + ":" + this.port);
            this.input = new ObjectInputStream(this.socket.getInputStream());
            this.output = new ObjectOutputStream(this.socket.getOutputStream());
        } catch (Exception e) {
            debug("connectToTracker: Couldn't add the streams.");
            e.printStackTrace();
        }
    }

    private void disconnect() {
        try {
            this.socket.close();
            this.socket = null;
            this.input.close();
            this.output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String sendTask() {
        TaskPacket packet;
        try {
            if (cmd.equals("job")) {
                packet = new TaskPacket(TaskPacket.SUBMIT, hash);
            } else {
                packet = new TaskPacket(TaskPacket.STATUS, hash);
            }
            this.output.writeObject(packet);
            packet = (TaskPacket) this.input.readObject();
            disconnect();
        } catch (Exception e) {
            return null;
        }
        return packet.result;
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
