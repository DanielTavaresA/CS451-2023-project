package cs451.Models;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Message implements Serializable {
    private MsgType type;
    private int seqNum;
    private int id;
    private byte[] data;

    private static final int TYPE_INDEX_SIZE = 4;
    private static final int SEQ_NUM_SIZE = 4;
    private static final int ID_SIZE = 4;

    public Message(MsgType type, int seqNum, byte[] data) {
        this.type = type;
        this.seqNum = seqNum;
        this.data = data;
        id = UUID.randomUUID().hashCode();
    }

    public Message(MsgType type, int seqNum, int id, byte[] data) {
        this.type = type;
        this.seqNum = seqNum;
        this.id = id;
        this.data = data;
    }

    public MsgType getType() {
        return type;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public byte[] getData() {
        return data;
    }

    public int getId() {
        return id;
    }

    public byte[] toBytes() {
        int size = TYPE_INDEX_SIZE + SEQ_NUM_SIZE + ID_SIZE + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(type.ordinal());
        buffer.putInt(seqNum);
        buffer.putInt(id);
        buffer.put(data);
        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        MsgType type = MsgType.values()[buffer.getInt()];
        int seqNum = buffer.getInt();
        int id = buffer.getInt();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new Message(type, seqNum, id, data);
    }

    @Override
    public String toString() {
        return "Message [data=" + new String(data) + ", seqNum=" + seqNum + ", type=" + type + ", id=" + id + "]";
    }
}