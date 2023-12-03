package cs451.Links;

import java.net.DatagramPacket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.HostIP;
import cs451.Models.Message;

/*
 * Class implementing a fair loss link. 
 * Properties of fair loss links should be satisfied.
 * - FL1 : Fair-loss : If a message is sent infinitely often then m is deliered infinitely often
 * - FL2 : Finite duplication : If a message is sent a finite number of times then m is delivered a finite number of times
 * - FL3 : No creation : No message is delivered unless it was sent
 */
public class FairLossLink implements Link, Subscriber<Message>, Publisher<Message> {

    private Subscription subscription;
    private final Logger logger = Logger.getLogger(FairLossLink.class.getName());
    private final SubmissionPublisher<Message> publisher;
    private final UDPHost host;

    /**
     * Constructor for the FairLossLink class.
     * Subscribes the host to this class and sets the logger level to OFF.
     * Initializes the executor and publisher for the class.
     *
     * @param host     The UDPHost object to be subscribed to this class.
     * @param executor The ExecutorService object to be initialized for this class.
     */
    public FairLossLink(UDPHost host, ExecutorService executor) {
        host.subscribe(this);
        this.host = host;
        logger.setLevel(Level.OFF);
        publisher = new SubmissionPublisher<>(executor, 256);

    }

    /**
     * Sends a message to a destination address and port using UDP protocol.
     * 
     * @param m    the message to be sent
     * @param dest the destination address of the message
     */
    @Override
    public void send(Message m, HostIP dest) {
        DatagramPacket packet = new DatagramPacket(m.toBytes(), m.toBytes().length, dest.getAddress(), dest.getPort());
        logger.log(Level.INFO, "[FLL] - Sending message : " + m.getId() + " to " + dest);
        host.send(packet);

    }

    /**
     * This method is used to deliver a message received through a DatagramPacket.
     * It logs the message ID and the sender's IP address and port.
     *
     * @param packet the DatagramPacket containing the message to be delivered
     */
    @Override
    public void deliver(Message msg) {
        logger.log(Level.INFO, "[FLL] - Delivering message : " + msg.getId() + " from "
                + msg.getSenderHostIP());
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    @Override
    public void onNext(Message item) {
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
    public void subscribe(Subscriber<? super Message> subscriber) {
        publisher.subscribe(subscriber);
    }

}
