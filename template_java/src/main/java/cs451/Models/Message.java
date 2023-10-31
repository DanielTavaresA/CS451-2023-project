package cs451.Models;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Message implements Serializable {

    private MsgType type;
    private int id;
    private int senderId;
    private int recieverId;
    private byte[] data;

    private static final int TYPE_INDEX_SIZE = 4;
    private static final int SENDER_ID_SIZE = 4;
    private static final int RECIEVER_ID_SIZE = 4;
    private static final int ID_SIZE = 4;

    public Message(MsgType type, int senderId, int recieverId, byte[] data) {
        this.type = type;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.data = data;
        id = UUID.randomUUID().hashCode();
    }

    private Message(MsgType type, int id, int senderId, int recieverId, byte[] data) {
        this.type = type;
        this.id = id;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.data = data;
    }

    public MsgType getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    public int getId() {
        return id;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getRecieverId() {
        return recieverId;
    }

    public byte[] toBytes() {
        int size = TYPE_INDEX_SIZE + SENDER_ID_SIZE + RECIEVER_ID_SIZE + ID_SIZE + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(type.ordinal());
        buffer.putInt(senderId);
        buffer.putInt(recieverId);
        buffer.putInt(id);
        buffer.put(data);
        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        MsgType type = MsgType.values()[buffer.getInt()];
        int senderId = buffer.getInt();
        int recieverId = buffer.getInt();
        int id = buffer.getInt();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new Message(type, id, senderId, recieverId, data);
    }

    public int getAckedId() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int ackedId = buffer.getInt();
        return ackedId;
    }

    public byte[] ackPayload() {
        ByteBuffer buffer = ByteBuffer.allocate(ID_SIZE);
        buffer.putInt(id);
        return buffer.array();
    }

    @Override
    public String toString() {
        return "Message [data=" + new String(data) + ", type=" + type + ", id=" + id + "]";
    }
}