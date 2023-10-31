package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.Message;
import cs451.Models.MsgType;

public class StubbornLink implements Link, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private FairLossLink fairLossLink;
    private ConcurrentHashMap<Integer, Message> delivered;
    private ConcurrentHashMap<Integer, ScheduledFuture<?>> waitForAck;
    private Set<Integer> acked;
    private Subscription subscription;
    private UDPHost host;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Logger logger = Logger.getLogger(StubbornLink.class.getName());
    private final SubmissionPublisher<DatagramPacket> publisher;

    public StubbornLink(UDPHost host, ExecutorService executor) {
        fairLossLink = new FairLossLink(host, executor);
        delivered = new ConcurrentHashMap<Integer, Message>();
        waitForAck = new ConcurrentHashMap<Integer, ScheduledFuture<?>>();
        acked = ConcurrentHashMap.newKeySet();
        fairLossLink.subscribe(this);
        this.host = host;
        this.executor = executor;
        publisher = new SubmissionPublisher<>(executor, 256);
        logger.setLevel(Level.OFF);
    }

    @Override
    public void send(Message m, UDPHost host, InetAddress dest, int port) {
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            if (!acked.contains(m.getId())) {
                logger.log(Level.INFO,
                        "[SBL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
                fairLossLink.send(m, host, dest, port);
            } else {
                logger.log(Level.INFO, "[SBL] - Message : " + m.getId() + " already acked");
                waitForAck.get(m.getId()).cancel(true);
                waitForAck.remove(m.getId());
            }
        }, 0, 2, TimeUnit.SECONDS);
        waitForAck.put(m.getId(), task);

    }

    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        logger.log(Level.INFO, "[SBL] - Delivering packet : " + msg.getId() + " from "
                + packet.getAddress().getHostAddress() + ":" + packet.getPort());

        switch (msg.getType()) {
            // sender side
            case ACK:

                int ackedId = msg.getAckedId();
                logger.log(Level.INFO, "[SBL] - Received ACK for message : " + ackedId);
                if (!acked.contains(ackedId)) {
                    acked.add(ackedId);
                    logger.log(Level.INFO, "[SBL] - Added ACK : " + acked.toString());
                }

                break;
            // reciever side
            case DATA:
                if (!delivered.containsKey(msg.getId())) {
                    delivered.put(msg.getId(), msg);
                    logger.log(Level.INFO,
                            "[SBL] - Received  : " + msg.getId() + " process PID : "
                                    + delivered.toString());

                    Message ack = new Message(MsgType.ACK, msg.getRecieverId(), msg.getSenderId(),
                            msg.ackPayload());
                    send(ack, host, packet.getAddress(), packet.getPort());
                    publisher.submit(packet);
                }

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
