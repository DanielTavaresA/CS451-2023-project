package cs451.Models;

import java.io.Serializable;
import java.util.UUID;

public class Metadata implements Serializable {
    private MsgType type;
    private int senderId;
    private int recieverId;
    private int id;
    private int seqNum;
    private IPAddress senderAddress;
    private IPAddress recieverAddress;

    public Metadata(MsgType type, int senderId, int recieverId, int seqNum, IPAddress senderAddress,
            IPAddress recieverAddress) {
        this.type = type;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.id = UUID.randomUUID().hashCode();
        this.seqNum = seqNum;
        this.senderAddress = senderAddress;
        this.recieverAddress = recieverAddress;
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

    public IPAddress getSenderAddress() {
        return senderAddress;
    }

    public IPAddress getRecieverAddress() {
        return recieverAddress;
    }

    public String toString() {
        return "Metadata: " + "type : " + type + " senderId : " + senderId + " recieverId : " + recieverId + " id : "
                + id + " seqNum : " + seqNum + " senderAddress : " + senderAddress + " recieverAddress : "
                + recieverAddress;

    }

}
