package cs451.Agreement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Broadcast.BestEffortBroadcast;
import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
import cs451.Models.Proposal;
import cs451.Parser.Host;
import cs451.utils.Log;

public class LatticeAgreement implements Agreement, Subscriber<Message> {

    private boolean active = false;
    private int ackCount = 0;
    private int nackCount = 0;
    private int activePropose = 0;
    private Set<Integer> proposedValues = new HashSet<Integer>();
    private Set<Integer> acceptedValues = new HashSet<Integer>();
    private Subscription subscription;
    private Logger logger = Logger.getLogger(LatticeAgreement.class.getName());

    private HostIP myHostIP;
    private ExecutorService executor;
    private BestEffortBroadcast beb;
    private PerfectLink pl;
    private Set<HostIP> destinations;
    private Queue<Set<Integer>> pending;

    public LatticeAgreement(UDPHost host, ExecutorService executor, Set<HostIP> destinations) {
        this.executor = executor;
        myHostIP = host.getHostIP();
        beb = new BestEffortBroadcast(host, destinations, executor);
        this.destinations = destinations;
        pl = new PerfectLink(host, executor);
        pending = new ConcurrentLinkedDeque<Set<Integer>>();
        beb.subscribe(this);
        pl.subscribe(this);
        logger.setLevel(Level.INFO);
    }

    @Override
    public void propose(Set<Integer> proposal) {
        if (active) {
            pending.add(proposal);
            return;
        }
        proposedValues = proposal;
        active = true;
        activePropose++;
        ackCount = 0;
        nackCount = 0;
        logger.info("[LA] - Proposing : " + proposal.toString() + " with id : " + activePropose);
        Message m = prepareProposalMsg(proposal);
        beb.broadcast(empack(m));
    }

    private Message prepareProposalMsg(Set<Integer> proposal) {
        Metadata metadata = new Metadata(MsgType.PROPOSAL, myHostIP.getId(), 0, 0, myHostIP, null);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            Proposal p = new Proposal(activePropose, proposal);
            outputStream.writeObject(p);
            outputStream.flush();
            outputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] data = bos.toByteArray();
        return new Message(metadata, data);
    }

    private byte[] prepareNackData(int proposalNb, Set<Integer> proposal) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            Proposal p = new Proposal(proposalNb, proposal);
            outputStream.writeObject(p);
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }

    private Proposal recoverProposal(Message m) {
        ByteArrayInputStream bis = new ByteArrayInputStream(m.getData());
        ObjectInputStream inputStream;
        Proposal proposal = null;
        try {
            inputStream = new ObjectInputStream(bis);
            proposal = (Proposal) inputStream.readObject();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return proposal;
    }

    private Message empack(Message m) {
        Metadata metadata = new Metadata(MsgType.DATA, m.getSenderHostIP().getId(),
                (m.getRecieverHostIP() == null) ? 0 : m.getRecieverHostIP().getId(), m.getSeqNum(), m.getSenderHostIP(),
                m.getRecieverHostIP());
        return new Message(metadata, m.toBytes());
    }

    private Message unpack(Message m) {
        return Message.fromBytes(m.getData());
    }

    @Override
    public void decide(Set<Integer> values) {
        logger.log(Level.INFO, "Decided : " + values.toString());
        String toLog = values.stream().map(Object::toString).reduce((a, b) -> a + " " + b).get() + "\n";
        Log.logFile(toLog);
        if (!pending.isEmpty()) {
            propose(pending.remove());
        }
    }

    private void processProposal(Message msg) {
        HostIP sender = msg.getSenderHostIP();
        Proposal proposal = recoverProposal(msg);
        logger.info("[LA] - Processing proposal : " + proposal.toString());
        if (proposal.getProposedValues().containsAll(acceptedValues)) {
            acceptedValues.addAll(proposal.getProposedValues());
            Metadata metadata = new Metadata(MsgType.ACK, myHostIP.getId(), sender.getId(), 0, myHostIP, sender);
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(proposal.getActivePropose());
            Message ack = new Message(metadata, buffer.array());
            pl.send(empack(ack), sender);
        } else {
            acceptedValues.addAll(proposal.getProposedValues());
            Metadata metadata = new Metadata(MsgType.NACK, myHostIP.getId(), sender.getId(), 0, myHostIP, sender);
            Message nack = new Message(metadata, prepareNackData(proposal.getActivePropose(), acceptedValues));
            pl.send(empack(nack), sender);

        }
    }

    private void processAck(Message msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getData());
        int proposalNb = buffer.getInt();
        logger.info("[LA] - Processing ACK : " + msg.getMetadata().toString() + " with proposal : "
                + proposalNb);
        if (proposalNb == activePropose) {
            ackCount++;
            if (ackCount > destinations.size() / 2 && active) {
                active = false;
                decide(proposedValues);
            }
        }
    }

    private void processNack(Message msg) {
        Proposal proposal = recoverProposal(msg);
        logger.info("[LA] - Processing NACK : " + msg.getMetadata().toString() + " with proposal : "
                + proposal.toString());
        if (proposal.getActivePropose() == activePropose) {
            nackCount++;
            proposedValues.addAll(proposal.getProposedValues());
            if (nackCount + ackCount > destinations.size() / 2 && active) {
                activePropose++;
                nackCount = 0;
                ackCount = 0;
                Message m = prepareProposalMsg(proposedValues);
                beb.broadcast(empack(m));
            }
        }
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Message item) {
        if (item.getType() != MsgType.DATA) {
            subscription.request(1);
            return;
        }
        Message m = unpack(item);
        // logger.info("[LA] - Received message " + item.toString());
        switch (m.getType()) {
            case PROPOSAL:
                processProposal(m);
                break;
            case ACK:
                processAck(m);
                break;
            case NACK:
                processNack(m);
                break;
            default:
                break;
        }
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
