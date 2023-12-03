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
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
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
    private ConcurrentHashMap<HostIP, Set<Message>> forward;
    private ConcurrentHashMap<Message, Set<HostIP>> receivedMsgFromMap;
    private boolean mustLog = false;

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

    @Override
    public void broadcast(Message m) {
        // pfd.start();
        // adapt metadata for broadcast
        Metadata metadata = new Metadata(m.getType(), myHostIP.getId(), m.getSenderId(), m.getSeqNum(),
                myHostIP, m.getSenderHostIP());
        // empacks message to assure compatibility with lower layers
        Message msg = new Message(metadata, m.toBytes());
        logger.info("[URB] - Broadcasting message : " + msg.toString() + "\n [URB] - unpacked : " + m.toString());
        if (mustLog) {
            String log = "b " + m.getId() + "\n";
            Log.logFile(log);
        }
        beb.broadcast(msg);
    }

    @Override
    public void deliver(Message msgUnpack) {
        // unpacks message
        logger.info("[URB] -  Delivering message " + msgUnpack.toString());
        if (mustLog) {
            String log = "d " + msgUnpack.getSenderId()
                    + msgUnpack.getId() + "\n";
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
        // unpacks message
        Message msgUnpack = Message.fromBytes(item.getData());
        logger.info("[URB] -  Received message : " + item.toString() + "\n [URB] - unpacked : " + msgUnpack.toString());
        receivedMsgFromMap.putIfAbsent(msgUnpack, new HashSet<HostIP>());

        // adds sender to the set of hosts that sent the message
        receivedMsgFromMap.get(msgUnpack).add(item.getSenderHostIP());

        logger.info("[URB] -    receivedMsgFromMap : " + receivedMsgFromMap.toString());
        logger.info("[URB] -  forward : " + forward.toString());

        // adds message to the set of messages to forward if it is not already in it and
        // broadcasts it
        if (!(forward.get(msgUnpack.getSenderHostIP()).contains(msgUnpack))) {
            forward.get(msgUnpack.getSenderHostIP()).add(msgUnpack);
            logger.info("[URB] -  adding to forward" + forward.toString());
            Metadata metadata = new Metadata(msgUnpack.getType(), myHostIP.getId(), msgUnpack.getSenderId(),
                    msgUnpack.getSeqNum(),
                    myHostIP, msgUnpack.getSenderHostIP());
            Message msg = new Message(metadata, msgUnpack.toBytes());
            beb.broadcast(msg);
        }
        checkDeliver(msgUnpack);
        subscription.request(1);
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
        if (!delivered.contains(m) && containsMajority(m)) {
            delivered.add(m);
            deliver(m);
        }
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
