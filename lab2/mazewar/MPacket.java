import java.io.Serializable;

public class MPacket implements Serializable {

    /*The following are the type of events*/
    public static final int HELLO = 100;
    public static final int ACTION = 200;

    /*The following are the specific action 
    for each type*/
    /*Initial Hello*/
    public static final int HELLO_INIT = 101;
    /*Response to Hello*/
    public static final int HELLO_RESP = 102;

    /*Action*/
    public static final int UP = 201;
    public static final int DOWN = 202;
    public static final int LEFT = 203;
    public static final int RIGHT = 204;
    public static final int FIRE = 205;
    
    //These fields characterize the event  
    public int type;
    public int event;
    public VectorClock clock;

    //The name determines the client that initiated the event
    public String name;
    
    //The sequence number of the event
    public int sequenceNumber;

    //These are used to initialize the board
    public int mazeSeed;
    public int mazeHeight;
    public int mazeWidth; 
    public Player[] players;

    public MPacket(int type, int event) {
        this.type = type;
        this.event = event;
    }

    public MPacket(String name, int type, int event) {
        this.name = name;
        this.type = type;
        this.event = event;
    }

    public MPacket(String name, int type, int event, VectorClock vc) {
        this.name = name;
        this.type = type;
        this.event = event;
        this.clock = vc;
    }
    
    public String toString(){
        String typeStr;
        String eventStr;
        
        switch(type){
            case 100:
                typeStr = "HELLO";
                break;
            case 200:
                typeStr = "ACTION";
                break;
            default:
                typeStr = "ERROR";
                break;        
        }
        switch(event){
            case 101:
                eventStr = "HELLO_INIT";
                break;
            case 102:
                eventStr = "HELLO_RESP";
                break;
            case 201:
                eventStr = "UP";
                break;
            case 202:
                eventStr = "DOWN";
                break;
            case 203:
                eventStr = "LEFT";
                break;
            case 204:
                eventStr = "RIGHT";
                break;
            case 205:
                eventStr = "FIRE";
                break;
            default:
                eventStr = "ERROR";
                break;        
        }
        //MPACKET(NAME: name, <typestr: eventStr>, SEQNUM: sequenceNumber, VC: this.clock)
        String retString = String.format(
                "MPACKET(NAME: %s, <%s: %s>, SEQNUM: %s, VC: %s)",
                name,
                typeStr,
                eventStr,
                sequenceNumber,
                this.clock);
        return retString;
    }

}
