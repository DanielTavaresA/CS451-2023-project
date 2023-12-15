package cs451.Broadcast;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
import cs451.utils.Log;

/**
 * .
 * UniformReliableBroadcast class provides a reliable broadcast mechanism by
 * building on top of the BestEffortBroadcast class.
 * Messages are broadcasted uniformly to a set of destinations and delivered to
 * all destinations reliably.
 */
public class UniformReliableBroadcast implements Broadcaster, Subscriber<Message>, Publisher<Message> {

    private Subscription subscription;
    private SubmissionPublisher<Message> publisher;
    private HostIP myHostIP;
    private BestEffortBroadcast beb;
    private Logger logger = Logger.getLogger(UniformReliableBroadcast.class.getName());
    private ExecutorService executor;
    // private PerfectFailureDetector pfd;
    private Set<HostIP> destinations;
    private Set<Message> delivered;
    private Lock deliverdLock = new ReentrantLock();
    private ConcurrentHashMap<HostIP, Set<Message>> forward;
    private Lock forwardLock = new ReentrantLock();
    private ConcurrentHashMap<Message, Set<HostIP>> receivedMsgFromMap;
    private Lock msgLock = new ReentrantLock();
    private boolean mustLog = false;
    private boolean logTimestamp = false;

    public UniformReliableBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        beb = new BestEffortBroadcast(host, destinations, executor);
        myHostIP = host.getHostIP();
        beb.subscribe(this);
        this.executor = executor;
        this.destinations = destinations;
        publisher = new SubmissionPublisher<Message>(executor, 256);
        delivered = new HashSet<Message>();
        receivedMsgFromMap = new ConcurrentHashMap<Message, Set<HostIP>>();
        forward = new ConcurrentHashMap<HostIP, Set<Message>>();
        for (HostIP dest : destinations) {
            forward.put(dest, new HashSet<Message>());
        }

        // pfd = new PerfectFailureDetector(host, destinations, executor);
        logger.setLevel(Level.OFF);

    }

    public void activateLogging() {
        mustLog = true;
    }

    public void activateTimestampLogging() {
        logTimestamp = true;
    }

    @Override
    public void broadcast(Message m) {
        // pfd.start();
        // adapt metadata for broadcast
        Metadata metadata = new Metadata(MsgType.DATA, myHostIP.getId(), m.getSenderId(), m.getSeqNum(),
                myHostIP, m.getSenderHostIP());
        // empacks message to assure compatibility with lower layers
        Message msg = new Message(metadata, m.toBytes());
        // logger.info("[URB] - Broadcasting message : " + msg.toString() + "\n [URB] -
        // unpacked : " + m.toString());
        if (mustLog) {
            String log = "b " + m.getSeqNum() + "\n";
            if (logTimestamp)
                log += "t " + System.currentTimeMillis() + "\n";
            Log.logFile(log);
        }
        beb.broadcast(msg);
    }

    @Override
    public void deliver(Message msgUnpack) {
        // unpacks message
        logger.warning("[URB] -  Delivering message " + msgUnpack.toString());
        if (mustLog) {
            String log = "d " + msgUnpack.getSenderId() + " "
                    + msgUnpack.getSeqNum() + "\n";
            if (logTimestamp)
                log += "t " + System.currentTimeMillis() + "\n";
            Log.logFile(log);
        }
        publisher.submit(msgUnpack);

    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Message item) {
        executor.submit(() -> {
            process(item);
            subscription.request(1);
        });

    }

    private void process(Message item) {
        // unpacks message
        Message msgUnpack = Message.fromBytes(item.getData());
        logger.info("[URB] - unpacked : " + msgUnpack.toString() + " from " + item.getSenderHostIP().toString());

        // lock

        msgLock.lock();
        receivedMsgFromMap.putIfAbsent(msgUnpack, new HashSet<HostIP>());
        // adds sender to the set of hosts that sent the message
        receivedMsgFromMap.get(msgUnpack).add(item.getSenderHostIP());
        msgLock.unlock();
        // unlock

        logger.info("[URB] -    receivedMsgFromMap : " + receivedMsgFromMap.toString());
        logger.info("[URB] -  forward : " + forward.toString());

        // adds message to the set of messages to forward if it is not already in it and
        // broadcasts it
        forwardLock.lock();
        if (!(forward.get(msgUnpack.getSenderHostIP()).contains(msgUnpack))) {
            forward.get(msgUnpack.getSenderHostIP()).add(msgUnpack);

            logger.info("[URB] -  adding to forward" + forward.toString());
            Metadata metadata = new Metadata(MsgType.DATA, myHostIP.getId(), msgUnpack.getSenderId(),
                    msgUnpack.getSeqNum(),
                    myHostIP, msgUnpack.getSenderHostIP());
            Message msg = new Message(metadata, msgUnpack.toBytes());
            beb.broadcast(msg);
        }
        forwardLock.unlock();
        checkDeliver(msgUnpack);

    }

    /**
     * Checks if a message can be delivered and delivers it if it can
     * 
     * @param m    message to check
     * @param item datagram packet containing the message
     */
    private void checkDeliver(Message m) {
        logger.info("[URB] - checking if message can be delivered " + m.toString() + " "
                + receivedMsgFromMap.get(m).toString() + " " + destinations.toString() + " " + delivered.toString());
        deliverdLock.lock();
        if (!delivered.contains(m) && containsMajority(m)) {
            delivered.add(m);
            deliver(m);
        }
        deliverdLock.unlock();
    }

    private boolean containsMajority(Message m) {
        return receivedMsgFromMap.get(m).size() > destinations.size() / 2;
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
