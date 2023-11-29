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

public class FIFOBroadcast implements Broadcaster, Subscriber<DatagramPacket> {

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
        logger.setLevel(Level.INFO);
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
    public void deliver(DatagramPacket pkt) {
        Message pack = Message.fromBytes(pkt.getData());
        Message m = Message.fromBytes(pack.getData());
        FIFOMessage unpacked = unpackMessage(m);
        Message receivedMessage = unpacked.getMessage();
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
    public void onNext(DatagramPacket item) {
        Message pack = Message.fromBytes(item.getData());
        Message m = Message.fromBytes(pack.getData());
        FIFOMessage unpacked = unpackMessage(m);
        Message receivedMessage = unpacked.getMessage();
        logger.info("[FIFO] - Received message " + receivedMessage.toString());
        ConcurrentHashMap<HostIP, Set<Message>> receivedPast = unpacked.getPast();
        logger.info("[FIFO] - Received past " + receivedPast.toString());
        if (!delivered.contains(receivedMessage)) {
            for (HostIP src : receivedPast.keySet()) {
                List<Message> toDeliver = new ArrayList<Message>();
                toDeliver = receivedPast.get(src).stream().filter(msg -> msg.getSeqNum() >= next.get(src))
                        .sorted((m1, m2) -> m1.getSeqNum() - m2.getSeqNum()).collect(Collectors.toList());
                logger.info("[FIFO] - toDeliver : " + toDeliver.toString());
                for (Message msg : toDeliver) {
                    if (!delivered.contains(msg) && msg.getSeqNum() == next.get(src)) {
                        Message empack = prepareMessage(msg);
                        DatagramPacket pkt = new DatagramPacket(empack.toBytes(), empack.toBytes().length,
                                msg.getSenderHostIP().getAddress(), msg.getSenderHostIP().getPort());
                        deliver(pkt);
                        delivered.add(msg);
                        past.get(msg.getSenderHostIP()).add(msg);
                        next.put(src, next.get(src) + 1);
                    }
                }

            }
            deliver(item);
            delivered.add(receivedMessage);
            past.get(receivedMessage.getSenderHostIP()).add(receivedMessage);
            next.put(receivedMessage.getSenderHostIP(), next.get(receivedMessage.getSenderHostIP()) + 1);
        }
        subscription.request(1);
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
