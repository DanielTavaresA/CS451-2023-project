package cs451.Links;

import java.net.DatagramPacket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.MsgType;
import cs451.utils.Log;

/**
 * Class implementing a perfect link.
 * Properties of perfect links should be satisfied.
 * - Validity : if p_i and p_j are correct then every message sent by p_i to p_j
 * is eventually delivered by p_j
 * - No duplication : no message is delivered more than once
 * - No creation : No message is delivered unless it was sent
 */
public class PerfectLink implements Link, Subscriber<Message>, Publisher<Message> {

    private Subscription subscription;
    private Logger logger = Logger.getLogger(PerfectLink.class.getName());
    private StubbornLink stubbornLink;
    private ConcurrentHashMap<Integer, Message> sent;
    private ConcurrentHashMap<Integer, Message> delivered;
    private ExecutorService executor;
    private SubmissionPublisher<Message> publisher;
    private boolean mustLog = false;

    /**
     * Constructor for the PerfectLink class.
     * Initializes a StubbornLink object, subscribes to it, and initializes
     * ConcurrentHashMaps for sent and delivered messages.
     * 
     * @param host     the UDPHost object to use for communication
     * @param executor the ExecutorService object to use for running threads
     */
    public PerfectLink(UDPHost host, ExecutorService executor) {
        stubbornLink = new StubbornLink(host, executor);
        stubbornLink.subscribe(this);
        sent = new ConcurrentHashMap<Integer, Message>();
        delivered = new ConcurrentHashMap<Integer, Message>();
        this.executor = executor;
        publisher = new SubmissionPublisher<Message>(executor, 256);
        logger.setLevel(Level.OFF);
    }

    public void activateLogging() {
        mustLog = true;
    }

    /**
     * Sends a message using the Perfect Link protocol.
     * 
     * @param m    the message to be sent
     * @param dest the destination IP address of the message
     */
    @Override
    public void send(Message m, HostIP dest) {
        executor.submit(() -> stubbornLink.send(m, dest));
        sent.put(m.getId(), m);
        logger.log(Level.INFO, "[PL] - Sending message : " + m.getId() + " to " + dest);
        logger.log(Level.INFO, "[PL] - Sent message : " + m.getId() + " to " + dest);
        if (mustLog) {
            String log = "b " + new String(m.getData()).trim() + "\n";
            Log.logFile(log);
        }
    }

    /**
     * This method delivers a message by deserializing the data from the given
     * DatagramPacket,
     * storing it in the delivered HashMap with the corresponding ackedId as the
     * key,
     * and logging the delivery in the log file.
     * 
     * @param packet the DatagramPacket containing the serialized message to be
     *               delivered
     */
    @Override
    public void deliver(Message msg) {
        if (msg.getType() == MsgType.ACK) {
            logger.log(Level.INFO, "[PL] - Received ACK for message : " + msg.getAckedId());
            publisher.submit(msg);
            return;
        }
        int ackedId = msg.getId();
        logger.log(Level.INFO, "[PL] - Delivering message : " + msg.getId() + " from "
                + msg.getSenderHostIP());
        delivered.put(ackedId, msg);
        if (mustLog) {
            String log = "d " + msg.getSenderId() + " " + new String(msg.getData()).trim() + "\n";
            Log.logFile(log);
        }
        publisher.submit(msg);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Message item) {
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
    public void subscribe(Subscriber<? super Message> subscriber) {
        publisher.subscribe(subscriber);
    }

}