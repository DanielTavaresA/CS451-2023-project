package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

import cs451.Models.Message;
import cs451.Models.MsgType;

/**
 * Class implementing a perfect link.
 * Properties of perfect links should be satisfied.
 * - Validity : if p_i and p_j are correct then every message sent by p_i to p_j
 * is eventually delivered by p_j
 * - No duplication : no message is delivered more than once
 * - No creation : No message is delivered unless it was sent
 */
public class PerfectLink implements Link {

    UDPHost src;
    int destPort;
    InetAddress destAddress;

    public PerfectLink(UDPHost src, int destPort, String ip) {
        this.src = src;
        this.destPort = destPort;
        try {
            this.destAddress = InetAddress.getByName(ip);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* */
    @Override
    public CompletableFuture<Boolean> send(Message m, UDPHost host, InetAddress dest, int port) {
        return null;
        /*
         * byte[] bytes = m.toBytes();
         * DatagramPacket packet = new DatagramPacket(bytes, bytes.length, destAddress,
         * destPort);
         * return src.send(packet);
         */
    }

    @Override
    public CompletableFuture<Boolean> deliver(DatagramPacket packet) {
        return null;
        /*
         * DatagramPacket packet = src.receive();
         * Message msg = Message.fromBytes(packet.getData());
         * 
         * switch (msg.getType()) {
         * case ACK:
         * break;
         * case DATA:
         * Message ackDataMsg = new Message(MsgType.ACK, msg.getSeqNum(), new byte[0]);
         * send(ackDataMsg, host, packet.getAddress(), packet.getPort());
         * break;
         * default:
         * return null;
         * }
         * return packet;
         */

    }

    /*
     * @Override
     * public void start() {
     * Message synMsg = new Message(MsgType.SYN, 0, new byte[0]);
     * if (send(synMsg)) {
     * if (receive()) {
     * System.out.println("Perfect link established between " +
     * src.getAddress().getHostAddress() + ":"
     * + src.getPort() + " and " + destAddress.getHostAddress() + ":" + destPort);
     * }
     * }
     * }
     */

}