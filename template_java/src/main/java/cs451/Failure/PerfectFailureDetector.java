package cs451.Failure;

import java.net.DatagramPacket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
import cs451.utils.Log;

public class PerfectFailureDetector implements FailureDetector, Subscriber<DatagramPacket> {

    private ExecutorService executor;
    private PerfectLink pl;
    private Set<HostIP> endPoints;
    private HostIP myHostIP;
    private Subscription subscription;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ConcurrentHashMap<HostIP, ScheduledFuture<?>> sendProcess;
    private ConcurrentHashMap<HostIP, ScheduledFuture<?>> failureMonitor;
    private ConcurrentHashMap<HostIP, Set<Integer>> waitForAck;
    private ConcurrentHashMap<HostIP, Set<Integer>> receivedAck;
    private ConcurrentHashMap<HostIP, Long> timeoutMap;
    private Set<HostIP> suspected;
    private final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private final long BASIC_TIMEOUT = 500;
    private Logger logger = Logger.getLogger(PerfectFailureDetector.class.getName());

    public PerfectFailureDetector(UDPHost host, Set<HostIP> endPoints, ExecutorService executor) {
        this.pl = new PerfectLink(host, executor);
        this.executor = executor;
        this.endPoints = endPoints;
        this.myHostIP = host.getHostIP();
        this.failureMonitor = new ConcurrentHashMap<HostIP, ScheduledFuture<?>>();
        this.timeoutMap = new ConcurrentHashMap<HostIP, Long>();
        this.waitForAck = new ConcurrentHashMap<HostIP, Set<Integer>>();
        this.receivedAck = new ConcurrentHashMap<HostIP, Set<Integer>>();
        this.sendProcess = new ConcurrentHashMap<HostIP, ScheduledFuture<?>>();
        this.suspected = new HashSet<HostIP>();
        logger.setLevel(Level.INFO);

        for (HostIP hostIP : endPoints) {
            timeoutMap.put(hostIP, BASIC_TIMEOUT);
            waitForAck.put(hostIP, new HashSet<Integer>());
            receivedAck.put(hostIP, new HashSet<Integer>());
        }

    }

    @Override
    public void start() {
        for (HostIP hostIP : endPoints) {
            if (hostIP.getId() != myHostIP.getId()) {
                ScheduledFuture<?> heartbeatSend = scheduler.scheduleAtFixedRate(() -> {
                    logger.log(Level.INFO, "[PFD] - Sending heartbeat to " + hostIP.getId());
                    Metadata metadata = new Metadata(MsgType.HEARTBEAT, myHostIP.getId(), hostIP.getId(), 0, myHostIP,
                            hostIP);
                    Message msg = new Message(metadata, "heartbeat".getBytes());
                    pl.send(msg, hostIP);
                    waitForAck.get(hostIP).add(msg.getId());
                }, 0, timeoutMap.get(hostIP), TIME_UNIT);
                sendProcess.put(hostIP, heartbeatSend);

                ScheduledFuture<?> heartbeatMonitor = scheduler.scheduleAtFixedRate(() -> {
                    logger.log(Level.INFO, "[PFD] - Checking heartbeat from " + hostIP.getId());
                    Set<Integer> ackIntersect = new HashSet<Integer>(waitForAck.get(hostIP));
                    ackIntersect.retainAll(receivedAck.get(hostIP));
                    if (ackIntersect.size() != 0) {
                        for (Integer id : ackIntersect) {
                            waitForAck.get(hostIP).remove(id);
                            receivedAck.get(hostIP).remove(id);
                        }
                        if (suspected.contains(hostIP)) {
                            suspected.remove(hostIP);
                            timeoutMap.put(hostIP, timeoutMap.get(hostIP) * 2);
                        }
                    } else {
                        suspected.add(hostIP);
                    }

                }, 0, timeoutMap.get(hostIP), TIME_UNIT);
                failureMonitor.put(hostIP, heartbeatMonitor);
            }

        }
    }

    @Override
    public void stop() {
        for (HostIP hostIP : endPoints) {
            if (hostIP.getId() != myHostIP.getId()) {
                sendProcess.get(hostIP).cancel(true);
                failureMonitor.get(hostIP).cancel(true);
            }
        }
    }

    @Override
    public void crash() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'crash'");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    @Override
    public void onNext(DatagramPacket item) {
        Message msg = Message.fromBytes(item.getData());
        if (msg.getType() == MsgType.HEARTBEAT) {
            Metadata metadata = new Metadata(MsgType.HEARTBEAT_ACK, myHostIP.getId(), msg.getSenderId(),
                    msg.getId(), myHostIP, msg.getSenderHostIP());
            Message ack = new Message(metadata, null);
            pl.send(ack, msg.getSenderHostIP());
        } else if (msg.getType() == MsgType.HEARTBEAT_ACK) {
            receivedAck.get(msg.getSenderHostIP()).add(msg.getId());
        }
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
}
