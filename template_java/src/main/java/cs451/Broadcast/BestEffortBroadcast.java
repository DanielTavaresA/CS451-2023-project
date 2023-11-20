package cs451.Broadcast;

import java.net.DatagramPacket;
import java.util.Set;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Links.PerfectLink;
import cs451.Models.IPAddress;
import cs451.Models.Message;

public class BestEffortBroadcast implements Broadcaster, Subscriber<DatagramPacket> {

    private PerfectLink perfectLink;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(BestEffortBroadcast.class.getName());
    private Set<IPAddress> destinations;

    public BestEffortBroadcast(PerfectLink perfectLink, Set<IPAddress> destinations) {
        this.perfectLink = perfectLink;
        this.destinations = destinations;
        perfectLink.subscribe(this);
        logger.setLevel(Level.OFF);
    }

    @Override
    public void broadcast(Message m) {
        for (IPAddress dest : destinations) {
            perfectLink.send(m, dest);
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

}
