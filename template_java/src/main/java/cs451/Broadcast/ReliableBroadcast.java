package cs451.Broadcast;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Failure.PerfectFailureDetector;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Parser.Host;

public class ReliableBroadcast implements Broadcaster, Subscriber<DatagramPacket> {

    private Subscription subscription;
    private HostIP myHostIP;
    private BestEffortBroadcast beb;
    private Logger logger = Logger.getLogger(ReliableBroadcast.class.getName());
    private ExecutorService executor;
    private PerfectFailureDetector pfd;
    private Set<HostIP> destinations;
    private Set<Message> delivered;
    private ConcurrentHashMap<HostIP, Set<Message>> forward;
    private ConcurrentHashMap<Message, Set<HostIP>> ack;
    private ConcurrentHashMap<InetAddress, HostIP> idToHostIP;

    public ReliableBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        beb = new BestEffortBroadcast(host, destinations, executor);
        myHostIP = host.getHostIP();
        beb.subscribe(this);
        this.executor = executor;
        this.destinations = destinations;
        delivered = new HashSet<Message>();
        ack = new ConcurrentHashMap<Message, Set<HostIP>>();
        forward = new ConcurrentHashMap<HostIP, Set<Message>>();
        idToHostIP = new ConcurrentHashMap<InetAddress, HostIP>();
        for (HostIP dest : destinations) {
            forward.put(dest, new HashSet<Message>());
            idToHostIP.put(dest.getAddress(), dest);
        }

        // pfd = new PerfectFailureDetector(host, destinations, executor);
        logger.setLevel(Level.OFF);

    }

    @Override
    public void broadcast(Message m) {
        // pfd.start();
        Metadata metadata = new Metadata(m.getType(), myHostIP.getId(), m.getSenderId(), m.getSeqNum(),
                myHostIP, m.getSenderHostIP());
        Message msg = new Message(metadata, m.getData());
        forward.get(myHostIP).add(msg);
        beb.broadcast(msg);
    }

    @Override
    public void deliver(DatagramPacket pkt) {
        logger.info("[URB] -  Delivering packet");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(DatagramPacket item) {
        Message m = Message.fromBytes(item.getData());
        ack.putIfAbsent(m, new HashSet<HostIP>());
        ack.get(m).add(idToHostIP.get(item.getAddress()));
        if (!forward.get(m.getSenderHostIP()).contains(m)) {
            forward.get(m.getSenderHostIP()).add(m);
            beb.broadcast(m);
        }
        checkDeliver(idToHostIP.get(item.getAddress()), m, item);
        subscription.request(1);
    }

    private void checkDeliver(HostIP host, Message m, DatagramPacket item) {
        if (!delivered.contains(m) && ack.get(m).containsAll(destinations)) {
            logger.log(Level.INFO, "[RB] - Delivering message : " + m.getId());
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

}
