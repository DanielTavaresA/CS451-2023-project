package cs451.Models;

import java.io.Serializable;
import java.util.UUID;

public class Metadata implements Serializable {
    private MsgType type;
    private int senderId;
    private int recieverId;
    private int id;
    private int seqNum;
    private HostIP senderHostIP;
    private HostIP recieverHostIP;

    public Metadata(MsgType type, int senderId, int recieverId, int seqNum, HostIP senderHostIP,
            HostIP recieverHostIP) {
        this.type = type;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.id = UUID.randomUUID().hashCode();
        this.seqNum = seqNum;
        this.senderHostIP = senderHostIP;
        this.recieverHostIP = recieverHostIP;
    }

    public MsgType getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getRecieverId() {
        return recieverId;
    }

    public int getId() {
        return id;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public HostIP getSenderHostIP() {
        return senderHostIP;
    }

    public HostIP getRecieverHostIP() {
        return recieverHostIP;
    }

    public String toString() {
        return "Metadata: " + "type : " + type + " senderId : " + senderId + " recieverId : " + recieverId + " id : "
                + id + " seqNum : " + seqNum + " senderAddress : " + senderHostIP + " recieverAddress : "
                + recieverHostIP;

    }

}
