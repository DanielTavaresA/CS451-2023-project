package cs451.Models;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Message implements Serializable {

    private MsgType type;
    private int id;
    private int senderId;
    private byte[] data;

    private static final int TYPE_INDEX_SIZE = 4;
    private static final int SENDER_ID_SIZE = 4;
    private static final int ID_SIZE = 4;

    public Message(MsgType type, int senderId, byte[] data) {
        this.type = type;
        this.senderId = senderId;
        this.data = data;
        id = this.hashCode();
    }

    private Message(MsgType type, int id, int senderId, byte[] data) {
        this.type = type;
        this.id = id;
        this.senderId = senderId;
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

    public byte[] toBytes() {
        int size = TYPE_INDEX_SIZE + SENDER_ID_SIZE + ID_SIZE + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(type.ordinal());
        buffer.putInt(senderId);
        buffer.putInt(id);
        buffer.put(data);
        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        MsgType type = MsgType.values()[buffer.getInt()];
        int senderId = buffer.getInt();
        int id = buffer.getInt();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new Message(type, id, senderId, data);
    }

    public static int getAckedId(Message msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getData());
        int ackedId = buffer.getInt();
        return ackedId;
    }

    public static byte[] ackPayload(Message msg) {
        ByteBuffer buffer = ByteBuffer.allocate(ID_SIZE);
        buffer.putInt(msg.getId());
        return buffer.array();
    }

    @Override
    public String toString() {
        return "Message [data=" + new String(data) + ", type=" + type + ", id=" + id + "]";
    }
}