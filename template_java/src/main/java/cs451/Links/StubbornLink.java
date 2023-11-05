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

/**
 * Class implementing a stubborn link. Properties of stubborn links should be
 * satisfied.
 * SL1. Stubborn delivery: if a process pi sends a message m to a correct
 * process pj, and pi does not crash, then pj delivers m an infinite number of
 * times
 * SL2. No creation: No message is delivered unless it was sent
 */
public class StubbornLink implements Link, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private static final long TIME_PERIOD = 500;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private FairLossLink fairLossLink;
    private ConcurrentHashMap<Integer, Message> delivered;
    private ConcurrentHashMap<Integer, ScheduledFuture<?>> waitForAck;
    private Set<Integer> acked;
    private Subscription subscription;
    private UDPHost host;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Logger logger = Logger.getLogger(StubbornLink.class.getName());
    private final SubmissionPublisher<DatagramPacket> publisher;

    /**
     * This class represents a StubbornLink, which is a reliable link that
     * guarantees that a message will be delivered
     * to the destination at least once. It is built on top of the FairLossLink
     * class, which provides a best-effort
     * unreliable link. This class keeps track of delivered messages and waits for
     * acknowledgements from the destination.
     * If an acknowledgement is not received within a certain time frame, the
     * message is resent.
     * 
     * @param host     the UDPHost object representing the host of the link
     * @param executor the ExecutorService object used to execute tasks
     *                 asynchronously
     */
    public StubbornLink(UDPHost host, ExecutorService executor) {
        fairLossLink = new FairLossLink(host, executor);
        delivered = new ConcurrentHashMap<Integer, Message>();
        waitForAck = new ConcurrentHashMap<Integer, ScheduledFuture<?>>();
        acked = ConcurrentHashMap.newKeySet();
        fairLossLink.subscribe(this);
        this.host = host;
        publisher = new SubmissionPublisher<>(executor, 256);
        logger.setLevel(Level.OFF);
    }

    /**
     * Sends a message using the Stubborn Link protocol.
     * If the message has not been acknowledged, it resends the message periodically
     * until it is acknowledged.
     * If the message has already been acknowledged, it cancels the scheduled task
     * for resending the message.
     * 
     * @param m    the message to be sent
     * @param host the UDP host
     * @param dest the destination IP address
     * @param port the destination port number
     */
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
        }, 0, TIME_PERIOD, TIME_UNIT);
        waitForAck.put(m.getId(), task);
    }

    /**
     * This method is responsible for delivering the received packet. It extracts
     * the message from the packet and logs the
     * message id and the sender's address. Depending on the message type, it either
     * adds the message id to the list of
     * acknowledged messages or adds the message to the list of delivered messages
     * and sends an acknowledgement message to
     * the sender. It also submits the packet to the publisher.
     *
     * @param packet the packet to be delivered
     */
    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        logger.log(Level.INFO,
                "[SBL] - Received  : " + msg.getId());

        switch (msg.getType()) {
            // sender side
            case ACK:
                int ackedId = msg.getAckedId();
                if (!acked.contains(ackedId)) {
                    logger.log(Level.INFO, "[SBL] - Received ACK for message : " + ackedId);
                    acked.add(ackedId);
                    logger.log(Level.INFO, "[SBL] - Added ACK : " + ackedId);
                }

                break;
            // reciever side
            case DATA:
                if (!delivered.containsKey(msg.getId())) {
                    delivered.put(msg.getId(), msg);
                    logger.log(Level.INFO, "[SBL] - Delivering packet : " + msg.getId() + " from "
                            + packet.getAddress().getHostAddress() + ":" + packet.getPort());

                    Message ack = new Message(MsgType.ACK, msg.getRecieverId(), msg.getSenderId(),
                            msg.ackPayload());
                    fairLossLink.send(ack, host, packet.getAddress(), packet.getPort());
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
