import java.io.Serializable;

public class TaskPacket implements Serializable {

    // Packet Type
    public static final int SUBMIT = 100;
    public static final int STATUS = 101;
    public static final int RESPONSE = 102;

    public int packet_type;
    public String hash;
    public String result;

    public TaskPacket(Integer type, String hash) {
        this.packet_type = type;
        this.hash = hash;
    }

    public TaskPacket(Integer type, String hash, String result) {
        this.packet_type = type;
        this.hash = hash;
        this.result = result;
    }
}
