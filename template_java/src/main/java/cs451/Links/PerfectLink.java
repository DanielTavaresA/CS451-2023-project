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

import cs451.Models.IPAddress;
import cs451.Models.Message;
import cs451.utils.Log;

/**
 * Class implementing a perfect link.
 * Properties of perfect links should be satisfied.
 * - Validity : if p_i and p_j are correct then every message sent by p_i to p_j
 * is eventually delivered by p_j
 * - No duplication : no message is delivered more than once
 * - No creation : No message is delivered unless it was sent
 */
public class PerfectLink implements Link, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private Subscription subscription;
    private Logger logger = Logger.getLogger(PerfectLink.class.getName());
    private StubbornLink stubbornLink;
    private ConcurrentHashMap<Integer, Message> sent;
    private ConcurrentHashMap<Integer, Message> delivered;
    private ExecutorService executor;
    private UDPHost host;
    private Publisher<DatagramPacket> publisher;

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
        this.host = host;
        sent = new ConcurrentHashMap<Integer, Message>();
        delivered = new ConcurrentHashMap<Integer, Message>();
        this.executor = executor;
        publisher = new SubmissionPublisher<DatagramPacket>(executor, 256);
        logger.setLevel(Level.OFF);
    }

    /**
     * Sends a message using the Perfect Link protocol.
     * 
     * @param m    the message to be sent
     * @param dest the destination IP address of the message
     */
    @Override
    public void send(Message m, IPAddress dest) {
        executor.submit(() -> stubbornLink.send(m, dest));
        sent.put(m.getId(), m);
        logger.log(Level.INFO, "[PL] - Sending message : " + m.getId() + " to " + dest);
        String log = "b " + new String(m.getData()).trim() + "\n";
        Log.logFile(log);
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
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        int ackedId = msg.getId();
        logger.log(Level.INFO, "[PL] - Delivering message : " + msg.getId() + " from "
                + packet.getAddress().getHostAddress() + ":" + packet.getPort() + "at time : "
                + System.currentTimeMillis());
        delivered.put(ackedId, msg);
        String log = "d " + msg.getSenderId() + " " + new String(msg.getData()).trim() + "\n";
        Log.logFile(log);
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