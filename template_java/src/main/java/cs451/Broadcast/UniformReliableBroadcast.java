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
public class UniformReliableBroadcast implements Broadcaster, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private Subscription subscription;
    private SubmissionPublisher<DatagramPacket> publisher;
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
        publisher = new SubmissionPublisher<DatagramPacket>(executor, 256);
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
    public void deliver(DatagramPacket pkt) {
        // unpacks message
        Message pack = Message.fromBytes(pkt.getData());
        Message m = Message.fromBytes(pack.getData());
        logger.info("[URB] -  Delivering message " + m.toString());
        if (mustLog) {
            String log = "d " + m.getSenderId()
                    + m.getId() + "\n";
            Log.logFile(log);
        }
        System.out.println("[URB] -  Delivering message ");
        publisher.submit(pkt);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(DatagramPacket item) {
        // unpacks message
        Message pack = Message.fromBytes(item.getData());
        Message m = Message.fromBytes(pack.getData());
        logger.info("[URB] -  Received message : " + pack.toString() + "\n [URB] - unpacked : " + m.toString());
        receivedMsgFromMap.putIfAbsent(m, new HashSet<HostIP>());

        // adds sender to the set of hosts that sent the message
        HostIP sender = findHost(item.getAddress(), item.getPort());
        receivedMsgFromMap.get(m).add(sender);

        logger.info("[URB] -    receivedMsgFromMap : " + receivedMsgFromMap.toString());
        logger.info("[URB] -  forward : " + forward.toString());

        // adds message to the set of messages to forward if it is not already in it and
        // broadcasts it
        if (!(forward.get(m.getSenderHostIP()).contains(m))) {
            forward.get(m.getSenderHostIP()).add(m);
            logger.info("[URB] -  adding to forward" + forward.toString());
            Metadata metadata = new Metadata(m.getType(), myHostIP.getId(), m.getSenderId(), m.getSeqNum(),
                    myHostIP, m.getSenderHostIP());
            Message msg = new Message(metadata, m.toBytes());
            beb.broadcast(msg);
        }
        checkDeliver(m, item);
        subscription.request(1);
    }

    /**
     * Finds the host with the given address and port in the set of destinations
     * 
     * @param address host address
     * @param port    host port
     * @return the host with the given address and port
     */
    private HostIP findHost(InetAddress address, int port) {
        for (HostIP host : destinations) {
            if (Objects.equals(host.getAddress(), address) && host.getPort() == port) {
                return host;
            }
        }
        return null;
    }

    /**
     * Checks if a message can be delivered and delivers it if it can
     * 
     * @param m    message to check
     * @param item datagram packet containing the message
     */
    private void checkDeliver(Message m, DatagramPacket item) {
        logger.info("[URB] - checking if message can be delivered " + m.toString() + " "
                + receivedMsgFromMap.get(m).toString() + " " + destinations.toString() + " " + delivered.toString());
        if (!delivered.contains(m) && receivedMsgFromMap.get(m).containsAll(destinations)) {
            delivered.add(m);
            deliver(item);
        }
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
