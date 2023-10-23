package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import cs451.Models.Message;
import cs451.Models.MsgType;
import cs451.utils.AtomicSet;

public class StubbornLink implements Link, Subscriber<DatagramPacket> {

    private FairLossLink fairLossLink;
    private AtomicSet<Integer> ackedMessages;
    private Subscription subscription;
    private UDPHost host;
    // private final Duration timeout = Duration.ofSeconds(1);

    public StubbornLink(UDPHost host) {
        fairLossLink = new FairLossLink(host);
        ackedMessages = new AtomicSet<>();
        this.host = host;
    }

    @Override
    public CompletableFuture<Boolean> send(Message m, UDPHost host, InetAddress dest, int port) {
        /*while (!ackedMessages.contains(m.getId())) {
            try {
                fairLossLink.send(m, host, dest, port).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return CompletableFuture.completedFuture(false);
            }
        }
        return CompletableFuture.completedFuture(true);*/

        return fairLossLink.send(m, host, dest, port);
    }

    @Override
    public CompletableFuture<Boolean> deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());

        switch (msg.getType()) {
            case ACK:
                ackedMessages.add(msg.getId());
                break;
            case DATA:
                Message ack = new Message(MsgType.ACK, msg.getId(), new byte[0]);
                fairLossLink.send(ack, host, packet.getAddress(), packet.getPort());
                break;
            default:
                break;
        }
        return CompletableFuture.completedFuture(true);
    }


    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    @Override
    public void onNext(DatagramPacket item) {
        deliver(item);
        subscription.request(1);
        
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("Completed");
    }

}
