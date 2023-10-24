package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.Message;
import cs451.Models.MsgType;

/**
 * Class implementing a perfect link.
 * Properties of perfect links should be satisfied.
 * - Validity : if p_i and p_j are correct then every message sent by p_i to p_j
 * is eventually delivered by p_j
 * - No duplication : no message is delivered more than once
 * - No creation : No message is delivered unless it was sent
 */
public class PerfectLink implements Link, Subscriber<DatagramPacket> {

    private UDPHost host;
    private Set<Integer> msgSent;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(PerfectLink.class.getName());
    private StubbornLink stubbornLink;

    public PerfectLink(UDPHost host) {
        this.host = host;
        stubbornLink = new StubbornLink(host);
        stubbornLink.subscribe(this);
        msgSent = ConcurrentHashMap.newKeySet();
    }

    /* */
    @Override
    public void send(Message m, UDPHost host, InetAddress dest, int port) {
        stubbornLink.send(m, host, dest, port);
        logger.log(Level.INFO, "[PL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
    }

    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        if (!msgSent.contains(msg.getId())) {
            logger.log(Level.INFO, "[PL] - Delivering message : " + msg.getId() + " from "
                    + packet.getAddress().getHostAddress() + ":" + packet.getPort());
            msgSent.add(msg.getId());
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

}