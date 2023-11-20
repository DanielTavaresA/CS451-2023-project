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
import cs451.Models.IPAddress;
import cs451.Models.Message;

public class BestEffortBroadcast implements Broadcaster, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private PerfectLink perfectLink;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(BestEffortBroadcast.class.getName());
    private Set<IPAddress> destinations;
    private Publisher<DatagramPacket> publisher;
    private ExecutorService executor;

    public BestEffortBroadcast(UDPHost host, Set<IPAddress> destinations, ExecutorService executor) {
        this.perfectLink = new PerfectLink(host, executor);
        this.destinations = destinations;
        perfectLink.subscribe(this);
        publisher = new SubmissionPublisher<DatagramPacket>(executor, 256);
        this.executor = executor;
        logger.setLevel(Level.OFF);
    }

    @Override
    public void broadcast(Message m) {
        for (IPAddress dest : destinations) {
            executor.submit(() -> perfectLink.send(m, dest));
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
