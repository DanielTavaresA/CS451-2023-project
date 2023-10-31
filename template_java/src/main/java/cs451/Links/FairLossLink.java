package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.Message;

/*
 * Class implementing a fair loss link. 
 * Properties of fair loss links should be satisfied.
 * - FL1 : Fair-loss : If a message is sent infinitely often then m is deliered infinitely often
 * - FL2 : Finite duplication : If a message is sent a finite number of times then m is delivered a finite number of times
 * - FL3 : No creation : No message is delivered unless it was sent
 */
public class FairLossLink implements Link, Subscriber<DatagramPacket>, Publisher<DatagramPacket> {

    private Subscription subscription;
    private ExecutorService executor;
    private final Logger logger = Logger.getLogger(FairLossLink.class.getName());
    private final SubmissionPublisher<DatagramPacket> publisher;

    public FairLossLink(UDPHost host, ExecutorService executor) {
        host.subscribe(this);
        logger.setLevel(Level.OFF);
        this.executor = executor;
        publisher = new SubmissionPublisher<>(executor, 256);

    }

    @Override
    public void send(Message m, UDPHost host, InetAddress dest, int port) {
        DatagramPacket packet = new DatagramPacket(m.toBytes(), m.toBytes().length, dest, port);
        logger.log(Level.INFO, "[FLL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
        host.send(packet);

    }

    @Override
    public void deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        logger.log(Level.INFO, "[FLL] - Delivering message : " + msg.getId() + " from "
                + packet.getAddress().getHostAddress() + ":" + packet.getPort());
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    @Override
    public void onNext(DatagramPacket item) {
        deliver(item);
        publisher.submit(item);
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
