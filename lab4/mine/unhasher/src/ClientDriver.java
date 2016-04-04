import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ClientDriver {

	static String id;	// used with znode /client/[id]
	static BufferedReader br = null;
	
	// ZooKeeper resources 
	ZkConnector zkc;
	static ZooKeeper zk;
	static String zkhost;
	static Integer zkport;
	static String lpath;
	
	// ZooKeeper directories
	static String ZK_CLIENTS = "/clients";
	static String ZK_TASK = "/tasks";
	static String ZK_SUBTASK = "/t";
    CountDownLatch regSig = new CountDownLatch(1);

	static boolean debug = true;

	public ClientDriver() {

		//connect to ZooKeeper
		zkc = new ZkConnector();
		try {			
			debug("Connecting to ZooKeeper instance zk");
			String hosts = String.format("%s:%d", zkhost, zkport);
			debug(hosts);
			zkc.connect(hosts);
			zk = zkc.getZooKeeper();
			debug("Connected to ZooKeeper instance zk");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void registerToService() {
				
		try {
			Stat stat = zk.exists(ZK_CLIENTS, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
					
					if (isNodeCreated) {
						regSig.countDown();
					} else {
						debug("huu?");
					}
				}
			});
			if (stat == null) {
				regSig.await();
			}
			
			String path = zk.create(ZK_CLIENTS + "/", 
					null, 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL_SEQUENTIAL);
			
			id = path.split("/")[2];
			
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public void sendPacket(TaskPacket p) {
		String data = p.taskToString();
		
		Code ret = createTaskPath(data);
				
		if (ret != Code.OK) { // debug("task sent!"); 
			System.out.println("request could not be sent!");
			return;
		}
		
		if (p.packet_type == TaskPacket.TASK_SUBMIT) {
			System.out.println("job submitted");
		}
	}
	
	private KeeperException.Code createTaskPath(String data) {
        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }
            lpath = zk.create(ZK_TASK + ZK_SUBTASK, 
            		byteData, 
            		ZooDefs.Ids.OPEN_ACL_UNSAFE, 
            		CreateMode.PERSISTENT_SEQUENTIAL);
            
        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        return KeeperException.Code.OK;
	}
	
	private String waitForStatus() {
		String path = lpath + "/res";
		
		byte [] data;
		String result = null;
		Stat stat = null;

		try {
			// wait for query result
			zkc.listenToPath(path);
			
			//result is back, get it
			data = zk.getData(path, false, stat);
			result = byteToString(data);
			zk.delete(path, 0);		//delete result from /tasks/t#
			zk.delete(lpath, 0);	// delete query /tasks
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
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

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String cmd = null;
        String hash = null;

		if(args.length == 3) {
			String[] s = args[0].split(":");
			zkhost = s[0];
			zkport = Integer.parseInt(s[1]);
			cmd = args[1];
			hash = args[2];
		} else {
			System.err.println("ERROR: Invalid arguments!");
			System.exit(-1);
		}

		ClientDriver cd = new ClientDriver();
		cd.registerToService();
		TaskPacket toZk = null;
		if (cmd.equals("job")) {
			toZk = new TaskPacket(id, TaskPacket.TASK_SUBMIT, hash);
			cd.sendPacket(toZk);
		} else if (cmd.equals("status")) {
			toZk = new TaskPacket(id, TaskPacket.TASK_QUERY, hash);
			cd.sendPacket(toZk);
			String result = cd.waitForStatus();
			System.out.println(result);
		}
	}

	private static void debug (String s) {
		if (debug) {
			System.out.println(String.format("CLIENT: %s", s ));
		}
	}


}
