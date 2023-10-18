package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import cs451.Models.Message;
import cs451.utils.AtomicSet;

public class StubbornLink implements Link {

    private FairLossLink fairLossLink;
    private AtomicSet<Integer> ackedMessages;
    // private final Duration timeout = Duration.ofSeconds(1);

    public StubbornLink() {
        fairLossLink = new FairLossLink();
        ackedMessages = new AtomicSet<>();
    }

    @Override
    public CompletableFuture<Boolean> send(Message m, UDPHost host, InetAddress dest, int port) {
        while (!ackedMessages.contains(m.getId())) {
            try {
                fairLossLink.send(m, host, dest, port).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return CompletableFuture.completedFuture(false);
            }
        }
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<DatagramPacket> deliver(UDPHost host) {
        DatagramPacket packet;
        try {
            packet = fairLossLink.deliver(host).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
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
        return CompletableFuture.completedFuture(packet);
    }

}
