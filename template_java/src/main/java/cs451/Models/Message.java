package cs451.Models;

import java.nio.ByteBuffer;

public class Message {
    private MsgType type;
    private int seqNum;
    private byte[] data;

    private static final int TYPE_SIZE = 4;
    private static final int SEQ_NUM_SIZE = 4;

    public Message(MsgType type, int seqNum, byte[] data) {
        this.type = type;
        this.seqNum = seqNum;
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

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(TYPE_SIZE + SEQ_NUM_SIZE + data.length);
        buffer.putInt(type.ordinal());
        buffer.putInt(seqNum);
        buffer.put(data);
        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        MsgType type = MsgType.values()[buffer.getInt()];
        int seqNum = buffer.getInt();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new Message(type, seqNum, data);
    }
}