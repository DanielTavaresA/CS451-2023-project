package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class PerfectLink implements Link, Subscriber<DatagramPacket> {

    private Subscription subscription;
    private Logger logger = Logger.getLogger(PerfectLink.class.getName());
    private StubbornLink stubbornLink;
    private ConcurrentHashMap<Integer, Message> sent;
    private ConcurrentHashMap<Integer, Message> delivered;
    private ExecutorService executor;

    public PerfectLink(UDPHost host, ExecutorService executor) {
        stubbornLink = new StubbornLink(host, executor);
        stubbornLink.subscribe(this);
        sent = new ConcurrentHashMap<Integer, Message>();
        delivered = new ConcurrentHashMap<Integer, Message>();
        this.executor = executor;
        logger.setLevel(Level.OFF);
    }

    /* */
    @Override
    public void send(Message m, UDPHost host, InetAddress dest, int port) {
        executor.submit(() -> stubbornLink.send(m, host, dest, port));
        sent.put(m.getId(), m);
        logger.log(Level.INFO, "[PL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
        String log = "b " + new String(m.getData()).trim() + "\n";
        Log.logFile(log);
    }

    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        int ackedId = msg.getAckedId();
        logger.log(Level.INFO, "[PL] - Delivering message : " + msg.getId() + " from "
                + packet.getAddress().getHostAddress() + ":" + packet.getPort());
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

}