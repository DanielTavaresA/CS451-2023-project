package cs451.Broadcast;

import java.net.DatagramPacket;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Failure.PerfectFailureDetector;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;

public class ReliableBroadcast implements Broadcaster, Subscriber<DatagramPacket> {

    private Subscription subscription;
    private BestEffortBroadcast beb;
    private Logger logger = Logger.getLogger(ReliableBroadcast.class.getName());
    private ExecutorService executor;
    private PerfectFailureDetector pfd;

    public ReliableBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        beb = new BestEffortBroadcast(host, destinations, executor);
        beb.subscribe(this);
        this.executor = executor;
        pfd = new PerfectFailureDetector(host, destinations, executor);
        logger.setLevel(Level.OFF);

    }

    @Override
    public void broadcast(Message m) {
        pfd.start();
    }

    @Override
    public void deliver(DatagramPacket pkt) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deliver'");
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
