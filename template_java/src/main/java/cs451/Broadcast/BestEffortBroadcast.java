package cs451.Broadcast;

import java.net.DatagramPacket;
import java.util.Set;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;

/**
 * BestEffortBroadcast class provides a best-effort broadcast mechanism using a
 * PerfectLink to send messages to multiple destinations.
 */
public class BestEffortBroadcast implements Broadcaster, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private PerfectLink perfectLink;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(BestEffortBroadcast.class.getName());
    private Set<HostIP> destinations;
    private SubmissionPublisher<DatagramPacket> publisher;
    private ExecutorService executor;

    public BestEffortBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        this.perfectLink = new PerfectLink(host, executor);
        this.destinations = destinations;
        perfectLink.subscribe(this);
        publisher = new SubmissionPublisher<DatagramPacket>(executor, 256);
        this.executor = executor;
        logger.setLevel(Level.OFF);
    }

    @Override
    public void broadcast(Message m) {
        for (HostIP dest : destinations) {
            Metadata metadata = new Metadata(m.getType(), m.getSenderId(), dest.getId(), m.getSeqNum(),
                    m.getSenderHostIP(), dest);
            Message msg = new Message(metadata, m.getData());
            logger.info("[BEB] - Sending message : " + msg.toString());
            executor.submit(() -> perfectLink.send(msg, dest));
        }
    }

    @Override
    public void deliver(DatagramPacket pkt) {
        Message m = Message.fromBytes(pkt.getData());
        logger.info("[BEB] - Delivering packet : " + m.toString());
        if (m.getType() == MsgType.DATA) {
            publisher.submit(pkt);
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

    @Override
    public void subscribe(Subscriber<? super DatagramPacket> subscriber) {
        publisher.subscribe(subscriber);
    }

}
