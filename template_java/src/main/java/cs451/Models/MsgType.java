package cs451.Models;

/**
 * This enum represents the different types of messages that can be sent between
 * processes in the distributed system.
 * 
 * The two possible message types are:
 * - ACK: a message sent to acknowledge receipt of a previous message
 * - DATA: a message containing data to be processed by the receiving process
 */
public enum MsgType {
    ACK, DATA, HEARTBEAT, HEARTBEAT_ACK;
}
