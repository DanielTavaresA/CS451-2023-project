package cs451.Models;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a message that can be sent between processes in a distributed
 * system.
 */
public class Message implements Serializable {

    private Metadata metadata;
    private byte[] data;

    /**
     * Constructs a new Message object with the given parameters.
     *
     * @param type       the type of the message
     * @param senderId   the ID of the process that sent the message
     * @param recieverId the ID of the process that should receive the message
     * @param data       the data to be sent with the message
     */
    public Message(Metadata metadata, byte[] data) {
        this.metadata = metadata;
        this.data = data;
    }

    /**
     * Converts the message to a byte array for transmission over the network.
     *
     * @return the message as a byte array
     */
    public byte[] toBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            outputStream.writeObject(this);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] metadataBytes = bos.toByteArray();
        return metadataBytes;
    }

    /**
     * Converts a byte array received over the network back into a Message object.
     *
     * @param bytes the byte array to convert
     * @return the Message object represented by the byte array
     */
    public static Message fromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream inputStream = new ObjectInputStream(bis);
            Message msg = (Message) inputStream.readObject();
            return msg;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * Returns the type of the message.
     *
     * @return the type of the message
     */
    public MsgType getType() {
        return metadata.getType();
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
        return metadata.getId();
    }

    /**
     * Returns the ID of the process that sent the message.
     *
     * @return the ID of the process that sent the message
     */
    public int getSenderId() {
        return metadata.getSenderId();
    }

    /**
     * Returns the ID of the process that should receive the message.
     *
     * @return the ID of the process that should receive the message
     */
    public int getRecieverId() {
        return metadata.getRecieverId();
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
     * Returns the sequence number of the message.
     *
     * @return the sequence number of the message
     */
    public int getSeqNum() {
        return metadata.getSeqNum();
    }

    /**
     * Returns the address of the process that sent the message.
     *
     * @return the address of the process that sent the message
     */
    public HostIP getSenderHostIP() {
        return metadata.getSenderHostIP();
    }

    /**
     * Returns the address of the process that should receive the message.
     * 
     * @return the address of the process that should receive the message
     */
    public HostIP getRecieverHostIP() {
        return metadata.getRecieverHostIP();
    }

    /**
     * Returns the payload of an acknowledgement message for this message.
     *
     * @return the payload of an acknowledgement message for this message
     */
    public byte[] ackPayload() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(getId());
        return buffer.array();
    }

    public Metadata getMetadata() {
        return metadata.copy();
    }

    public Metadata setMetadata(Metadata metadata) {
        Metadata oldMetadata = this.metadata;
        this.metadata = metadata;
        return oldMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(data), metadata.getId(), metadata.getSenderId(), metadata.getRecieverId(),
                metadata.getSeqNum(), metadata.getSenderHostIP(), metadata.getRecieverHostIP(), metadata.getType());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof Message)) {
            return false;
        }
        Message message = (Message) obj;
        return Arrays.equals(data, message.data) && Objects.equals(metadata, message.metadata);
    }

    @Override
    public String toString() {
        return "Message [data=" + new String(data) + ", type=" + getType() + ", id=" + getId() + ", senderId="
                + getSenderId() + ", recieverId=" + getRecieverId() + ", seqNum=" + getSeqNum() + ", senderAddress="
                + getSenderHostIP() + ", recieverAddress=" + getRecieverHostIP() + "]";
    }
}