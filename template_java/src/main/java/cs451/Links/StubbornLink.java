package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Set;

import cs451.Models.Message;

public class StubbornLink implements Link {

    private FairLossLink fairLossLink;
    private Set<Integer> ackedMessages;
    // private final Duration timeout = Duration.ofSeconds(1);

    public StubbornLink() {
        fairLossLink = new FairLossLink();
        ackedMessages = new java.util.HashSet<>();
    }

    @Override
    public boolean send(Message m, UDPHost host, InetAddress dest, int port) {
        while (!ackedMessages.contains(m.getId())) {
            fairLossLink.send(m, host, dest, port);
        }
        return true;
    }

    @Override
    public DatagramPacket deliver(UDPHost host) {
        DatagramPacket packet = fairLossLink.deliver(host);
        Message msg = Message.fromBytes(packet.getData());

        switch (msg.getType()) {
            case ACK:
                ackedMessages.add(msg.getId());
                break;
            case DATA:
                fairLossLink.send(msg, host, packet.getAddress(), packet.getPort());
                break;
            default:
                break;
        }
        return packet;
    }

}
