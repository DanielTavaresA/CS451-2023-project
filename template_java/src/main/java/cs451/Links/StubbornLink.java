package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.Message;
import cs451.Models.MsgType;

public class StubbornLink implements Link, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private FairLossLink fairLossLink;
    private Set<Integer> ackedMessages;
    private Subscription subscription;
    private UDPHost host;
    private final Logger logger = Logger.getLogger(StubbornLink.class.getName());
    private final SubmissionPublisher<DatagramPacket> publisher = new SubmissionPublisher<>();
    // private final Duration timeout = Duration.ofSeconds(1);

    public StubbornLink(UDPHost host) {
        fairLossLink = new FairLossLink(host);
        ackedMessages = ConcurrentHashMap.newKeySet();
        fairLossLink.subscribe(this);
        this.host = host;
    }

    @Override
    public void send(Message m, UDPHost host, InetAddress dest, int port) {
        CompletableFuture.runAsync(() -> {
            while (!ackedMessages.contains(m.getId())) {
                logger.log(Level.INFO,
                        "[SBL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
                fairLossLink.send(m, host, dest, port);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

    }

    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        logger.log(Level.INFO, "[SBL] - Delivering packet : " + msg.getId() + " from "
                + packet.getAddress().getHostAddress() + ":" + packet.getPort());

        switch (msg.getType()) {
            case ACK:
                ackedMessages.add(msg.getId());
                logger.log(Level.INFO, "[SBL] - Received ACK for message : " + msg.getId());
                break;
            case DATA:
                Message ack = new Message(MsgType.ACK, 0, msg.getId(), new byte[0]);
                fairLossLink.send(ack, host, packet.getAddress(), packet.getPort());
                break;
            default:
                break;
        }
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
        logger.log(Level.INFO, "Completed");
    }

    @Override
    public void subscribe(Subscriber<? super DatagramPacket> subscriber) {
        publisher.subscribe(subscriber);
    }

}
