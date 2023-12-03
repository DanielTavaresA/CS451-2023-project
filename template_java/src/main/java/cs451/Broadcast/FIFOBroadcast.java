package cs451.Broadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.utils.Log;

/**
 * .
 * FIFOBroadcast class provides a FIFO (First-In-First-Out) ordering guarantee
 * for message delivery.
 * Messages are broadcasted using the UniformReliableBroadcast protocol.
 * It keeps track of the past messages received from each sender and delivers
 * messages in the correct order.
 */
public class FIFOBroadcast implements Broadcaster, Subscriber<Message> {

    private Subscription subscription;
    private ConcurrentHashMap<HostIP, Set<Message>> past;
    private ConcurrentHashMap<HostIP, Integer> next;
    private Set<Message> delivered;
    private ExecutorService executor;
    private UniformReliableBroadcast urb;
    private Logger logger = Logger.getLogger(FIFOBroadcast.class.getName());
    private boolean mustLog = false;

    public FIFOBroadcast(UDPHost host, Set<HostIP> destinations, ExecutorService executor) {
        this.executor = executor;
        past = new ConcurrentHashMap<HostIP, Set<Message>>();
        next = new ConcurrentHashMap<HostIP, Integer>();
        delivered = new HashSet<Message>();
        for (HostIP dest : destinations) {
            past.put(dest, new HashSet<Message>());
            next.put(dest, 1);
        }
        urb = new UniformReliableBroadcast(host, destinations, executor);
        urb.subscribe(this);
        logger.setLevel(Level.OFF);
    }

    public void activateLogging() {
        mustLog = true;
    }

    @Override
    public void broadcast(Message m) {
        Message preparedMsg = prepareMessage(m);
        executor.submit(() -> urb.broadcast(preparedMsg));
        logger.info("[FIFO] - Broadcasting message " + m.toString());
        if (mustLog) {
            String log = "b " + m.getSeqNum() + "\n";
            Log.logFile(log);
        }
        past.get(m.getSenderHostIP()).add(m);

    }

    /**
     * Prepares a message to be sent by adding the past messages received from each
     * sender.
     * 
     * @param m the message to be sent
     * @return the prepared message
     */
    private Message prepareMessage(Message m) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            outputStream.writeObject(past);
            outputStream.writeObject(m);
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] data = bos.toByteArray();
        return new Message(m.getMetadata(), data);
    }

    /**
     * Unpacks a message in the FIFO broadcast protocol.
     * It retrieves the received message and the set of past messages received from
     * each host.
     */
    @SuppressWarnings("unchecked")
    private FIFOMessage unpackMessage(Message m) {
        ByteArrayInputStream bis = new ByteArrayInputStream(m.getData());
        ObjectInputStream inputStream;
        Message receivedMessage = null;
        ConcurrentHashMap<HostIP, Set<Message>> receivedPast = null;
        try {
            inputStream = new ObjectInputStream(bis);
            receivedPast = (ConcurrentHashMap<HostIP, Set<Message>>) inputStream.readObject();
            receivedMessage = (Message) inputStream.readObject();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new FIFOMessage(receivedMessage, receivedPast);

    }

    @Override
    public void deliver(Message receivedMessage) {
        logger.info("[FIFO] - Delivering message " + receivedMessage.toString());
        if (mustLog) {
            String log = "d " + receivedMessage.getSenderId() + " " + receivedMessage.getSeqNum() + "\n";
            Log.logFile(log);
        }

    }

    @Override
    public void onComplete() {
        logger.log(Level.INFO, "Completed");
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onNext(Message item) {
        executor.submit(() -> process(item));
        subscription.request(1);
    }

    private void process(Message item) {
        // unpack message
        FIFOMessage fifoMessage = unpackMessage(item);
        Message receivedMessage = fifoMessage.getMessage();
        logger.info("[FIFO] - Received message " + receivedMessage.toString());
        ConcurrentHashMap<HostIP, Set<Message>> receivedPast = fifoMessage.getPast();
        logger.info("[FIFO] - Received past " + receivedPast.toString());

        if (!delivered.contains(receivedMessage)) {
            // deliver the received message
            if (receivedMessage.getSeqNum() == next.get(receivedMessage.getSenderHostIP())) {
                deliver(receivedMessage);
                delivered.add(receivedMessage);
                past.get(receivedMessage.getSenderHostIP()).add(receivedMessage);
                next.put(receivedMessage.getSenderHostIP(), next.get(receivedMessage.getSenderHostIP()) + 1);
            }
            // for each host, checks which messages from past can be delivered
            for (HostIP src : receivedPast.keySet()) {
                List<Message> toDeliver = new ArrayList<Message>();
                toDeliver = receivedPast.get(src).stream().filter(msg -> msg.getSeqNum() >= next.get(src))
                        .sorted((m1, m2) -> m1.getSeqNum() - m2.getSeqNum()).collect(Collectors.toList());
                logger.info("[FIFO] - toDeliver : " + toDeliver.toString());
                // for each possible message from past, checks if it should be delivered
                for (Message msg : toDeliver) {
                    if (!delivered.contains(msg) && msg.getSeqNum() == next.get(src)) {
                        logger.info("[FIFO] - Delivering message from past " + msg.toString());
                        logger.info("[FIFO] - Next : " + next.toString());
                        deliver(msg);
                        // update
                        delivered.add(msg);
                        past.get(msg.getSenderHostIP()).add(msg);
                        next.put(src, next.get(src) + 1);
                    }
                }

            }
            // deliver the received message
            if (receivedMessage.getSeqNum() == next.get(receivedMessage.getSenderHostIP())) {
                deliver(receivedMessage);
                delivered.add(receivedMessage);
                past.get(receivedMessage.getSenderHostIP()).add(receivedMessage);
                next.put(receivedMessage.getSenderHostIP(), next.get(receivedMessage.getSenderHostIP()) + 1);
            }
        }
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    private class FIFOMessage {
        private Message message;
        private ConcurrentHashMap<HostIP, Set<Message>> past;

        public FIFOMessage(Message message, ConcurrentHashMap<HostIP, Set<Message>> past) {
            this.message = message;
            this.past = past;
        }

        public Message getMessage() {
            return message;
        }

        public ConcurrentHashMap<HostIP, Set<Message>> getPast() {
            return past;
        }
    }

}
