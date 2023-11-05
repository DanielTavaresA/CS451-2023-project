package cs451.Models;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Represents a message that can be sent between processes in a distributed
 * system.
 */
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

    /**
     * Constructs a new Message object with the given parameters.
     *
     * @param type       the type of the message
     * @param senderId   the ID of the process that sent the message
     * @param recieverId the ID of the process that should receive the message
     * @param data       the data to be sent with the message
     */
    public Message(MsgType type, int senderId, int recieverId, byte[] data) {
        this.type = type;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.data = data;
        id = UUID.randomUUID().hashCode();
    }

    /**
     * Represents a message exchanged between processes in the distributed system.
     * Private method to create a message with a given ID.
     * 
     * @param type       the type of the message
     * @param id         the unique identifier of the message
     * @param senderId   the identifier of the process that sent the message
     * @param recieverId the identifier of the process that should receive the
     *                   message
     * @param data       the data contained in the message
     */
    private Message(MsgType type, int id, int senderId, int recieverId, byte[] data) {
        this.type = type;
        this.id = id;
        this.senderId = senderId;
        this.recieverId = recieverId;
        this.data = data;
    }

    /**
     * Converts the message to a byte array for transmission over the network.
     *
     * @return the message as a byte array
     */
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

    /**
     * Converts a byte array received over the network back into a Message object.
     *
     * @param bytes the byte array to convert
     * @return the Message object represented by the byte array
     */
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

    /**
     * Returns the type of the message.
     *
     * @return the type of the message
     */
    public MsgType getType() {
        return type;
    }

    /**
     * Returns the data associated with the message.
     *
     * @return the data associated with the message
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Returns the ID of the message.
     *
     * @return the ID of the message
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the ID of the process that sent the message.
     *
     * @return the ID of the process that sent the message
     */
    public int getSenderId() {
        return senderId;
    }

    /**
     * Returns the ID of the process that should receive the message.
     *
     * @return the ID of the process that should receive the message
     */
    public int getRecieverId() {
        return recieverId;
    }

    /**
     * Returns the ID of the message that this message is acknowledging.
     *
     * @return the ID of the message that this message is acknowledging
     */
    public int getAckedId() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int ackedId = buffer.getInt();
        return ackedId;
    }

    /**
     * Returns the payload of an acknowledgement message for this message.
     *
     * @return the payload of an acknowledgement message for this message
     */
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