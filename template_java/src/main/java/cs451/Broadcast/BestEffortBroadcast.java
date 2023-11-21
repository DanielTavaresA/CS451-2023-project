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

public class BestEffortBroadcast implements Broadcaster, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private PerfectLink perfectLink;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(BestEffortBroadcast.class.getName());
    private Set<HostIP> destinations;
    private Publisher<DatagramPacket> publisher;
    private ExecutorService executor;

    public BestEffortBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        this.perfectLink = new PerfectLink(host, executor);
        this.destinations = destinations;
        perfectLink.subscribe(this);
        publisher = new SubmissionPublisher<DatagramPacket>(executor, 256);
        this.executor = executor;
        logger.setLevel(Level.INFO);
    }

    @Override
    public void broadcast(Message m) {
        for (HostIP dest : destinations) {
            Metadata metadata = new Metadata(m.getType(), m.getSenderId(), dest.getId(), m.getSeqNum(),
                    m.getSenderHostIP(), dest);
            Message msg = new Message(metadata, m.getData());
            executor.submit(() -> perfectLink.send(msg, dest));
        }
    }

    @Override
    public void deliver(DatagramPacket pkt) {
        logger.info("[BEB] Delivering packet");
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
