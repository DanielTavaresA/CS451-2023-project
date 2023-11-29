package cs451.Failure;

import java.net.DatagramPacket;
import java.util.HashSet;
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

/**
 * The PerfectFailureDetector class implements the FailureDetector interface and
 * acts as a perfect failure detector in a distributed system. It uses the
 * PerfectLink
 * class to send and receive heartbeat messages to detect failures.
 */
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
    private final long BASIC_TIMEOUT = 5000;
    private Logger logger = Logger.getLogger(PerfectFailureDetector.class.getName());

    public PerfectFailureDetector(UDPHost host, Set<HostIP> endPoints, ExecutorService executor) {
        this.pl = new PerfectLink(host, executor);
        pl.subscribe(this);
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

    /**
     * Starts the perfect failure detector by scheduling heartbeat sending and
     * monitoring tasks for each host.
     */
    @Override
    public void start() {
        for (HostIP hostIP : endPoints) {
            if (hostIP.getId() != myHostIP.getId()) {
                ScheduledFuture<?> heartbeatSend = scheduler.scheduleAtFixedRate(() -> {
                    sendHeartbeat(hostIP);
                }, 0, timeoutMap.get(hostIP), TIME_UNIT);
                sendProcess.put(hostIP, heartbeatSend);
                ScheduledFuture<?> heartbeatMonitor = scheduler.scheduleAtFixedRate(() -> {
                    monitorHearbeat(hostIP);
                }, timeoutMap.get(hostIP), timeoutMap.get(hostIP), TIME_UNIT);
                failureMonitor.put(hostIP, heartbeatMonitor);
            }

        }
    }

    /**
     * Sends a heartbeat message to the specified host.
     *
     * @param hostIP the host IP to send the heartbeat to
     */
    private void sendHeartbeat(HostIP hostIP) {
        logger.log(Level.INFO, "[PFD] - Sending heartbeat to " + hostIP.getId());
        Metadata metadata = new Metadata(MsgType.DATA, myHostIP.getId(), hostIP.getId(), 0, myHostIP,
                hostIP);
        Message msg = new Message(metadata, "heartbeat".getBytes());
        pl.send(msg, hostIP);
        waitForAck.get(hostIP).add(msg.getId());
    }

    /**
     * Monitors the heartbeat from a specific host.
     * If the heartbeat is received, updates the acknowledgement status and removes
     * the corresponding IDs from the wait and received acknowledgement sets.
     * Removes the host from the suspected set if it is in the suspected set and
     * adapts heartbeat schedule.
     * If the heartbeat is not received, adds the host to the suspected set.
     *
     * @param hostIP The host IP to monitor the heartbeat from.
     */
    private void monitorHearbeat(HostIP hostIP) {
        logger.log(Level.INFO, "[PFD] - Checking heartbeat from " + hostIP.getId());
        Set<Integer> ackIntersect = new HashSet<Integer>(waitForAck.get(hostIP));
        logger.info("[PFD] - wait for Ack " + ackIntersect.toString());
        logger.info("[PFD] - received Ack " + receivedAck.get(hostIP).toString());
        ackIntersect.retainAll(receivedAck.get(hostIP));
        logger.info("[PFD] - intersect " + ackIntersect.toString());
        if (ackIntersect.size() != 0) {
            for (Integer id : ackIntersect) {
                waitForAck.get(hostIP).remove(id);
                receivedAck.get(hostIP).remove(id);
            }
            if (suspected.contains(hostIP)) {
                suspected.remove(hostIP);
                timeoutMap.put(hostIP, timeoutMap.get(hostIP) * 2);
                scheduleHeartbeat(hostIP);
                logger.info("[PFD] - No longer suspected " + suspected.toString());
                logger.info("[PFD] - Timeout for " + hostIP.getId() + " is " + timeoutMap.get(hostIP));
            }
        } else {
            suspected.add(hostIP);
            logger.info("[PFD] - Suspected " + suspected.toString());
        }

    }

    /**
     * Schedules the heartbeat sending and monitoring tasks for a given host.
     *
     * @param hostIP The IP address of the host.
     */
    private void scheduleHeartbeat(HostIP hostIP) {
        sendProcess.get(hostIP).cancel(false);
        ScheduledFuture<?> heartbeatSend = scheduler.scheduleAtFixedRate(() -> {
            sendHeartbeat(hostIP);
        }, timeoutMap.get(hostIP), timeoutMap.get(hostIP), TIME_UNIT);
        sendProcess.put(hostIP, heartbeatSend);
        failureMonitor.get(hostIP).cancel(false);
        ScheduledFuture<?> heartbeatMonitor = scheduler.scheduleAtFixedRate(() -> {
            monitorHearbeat(hostIP);
        }, 2 * timeoutMap.get(hostIP), timeoutMap.get(hostIP), TIME_UNIT);
        failureMonitor.put(hostIP, heartbeatMonitor);
    }

    /**
     * Stops the perfect failure detector.
     * Cancels all scheduled tasks for sending messages and monitoring failures.
     */
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
        throw new UnsupportedOperationException("Unimplemented method 'crash'");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    /**
     * This method is called when a new DatagramPacket is received.
     * It processes the received message and updates the necessary data structures.
     * If the received message is an ACK, it adds the acknowledged ID to the
     * receivedAck set.
     * It also logs the received ACK and heartbeat information.
     * Finally, it requests one more item from the subscription.
     *
     * @param item The received DatagramPacket.
     */
    @Override
    public void onNext(DatagramPacket item) {
        Message msg = Message.fromBytes(item.getData());
        if (msg.getType() == MsgType.ACK) {
            Set<Integer> waitForAckSet = waitForAck.get(msg.getSenderHostIP());
            logger.info(waitForAckSet.toString());
            if (waitForAckSet.contains(msg.getAckedId()))
                receivedAck.get(msg.getSenderHostIP()).add(msg.getAckedId());
            logger.info("[PFD] - Received ack for " + msg.getSenderId() + " : "
                    + receivedAck.get(msg.getSenderHostIP()).toString());
            logger.log(Level.INFO, "[PFD] - Received heartbeat ack from " + msg.getSenderHostIP());
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
