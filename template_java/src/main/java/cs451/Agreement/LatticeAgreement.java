package cs451.Agreement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import cs451.Models.LatticeState;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
import cs451.Models.Proposal;
import cs451.Parser.Host;
import cs451.utils.Log;

public class LatticeAgreement implements Agreement, Subscriber<Message> {

    private Map<Integer, LatticeState> slots;
    private Integer cntSlot;
    private Subscription subscription;
    private Logger logger = Logger.getLogger(LatticeAgreement.class.getName());

    private HostIP myHostIP;
    private ExecutorService executor;
    private BestEffortBroadcast beb;
    private PerfectLink pl;
    private Set<HostIP> destinations;
    private Queue<Set<Integer>> pending;

    public LatticeAgreement(UDPHost host, ExecutorService executor, Set<HostIP> destinations) {
        cntSlot = 0;
        slots = new ConcurrentHashMap<Integer, LatticeState>();
        this.executor = executor;
        myHostIP = host.getHostIP();
        beb = new BestEffortBroadcast(host, destinations, executor);
        this.destinations = destinations;
        pl = new PerfectLink(host, executor);
        pending = new ConcurrentLinkedDeque<Set<Integer>>();
        beb.subscribe(this);
        pl.subscribe(this);
        logger.setLevel(Level.OFF);
    }

    @Override
    public void propose(Proposal proposal) {
        if (slots.get(proposal.getSlot()) == null) {
            LatticeState state = new LatticeState(proposal.getSlot(), proposal.getProposedValues());
            state.setActive(true);
            slots.put(proposal.getSlot(), state);
        }
        logger.info("[LA] - Proposing : " + proposal.toString());
        Message m = prepareProposalMsg(proposal);
        beb.broadcast(empack(m));
    }

    private Message prepareProposalMsg(Proposal proposal) {
        Metadata metadata = new Metadata(MsgType.PROPOSAL, myHostIP.getId(), 0, 0, myHostIP, null);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            outputStream.writeObject(proposal);
            outputStream.flush();
            outputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] data = bos.toByteArray();
        return new Message(metadata, data);
    }

    private byte[] prepareNackData(Proposal proposal, Set<Integer> acceptedValues) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(bos);
            Proposal p = new Proposal(proposal.getActivePropose(), acceptedValues, proposal.getSlot());
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
    public void decide(Proposal values) {
        logger.log(Level.INFO, "Decided : " + values.toString());
        String toLog = values.getProposedValues().stream().map(Object::toString).reduce((a, b) -> a + " " + b).get()
                + "\n";
        Log.addLog(values.getSlot(), toLog);
    }

    private void processProposal(Message msg) {
        HostIP sender = msg.getSenderHostIP();
        Proposal proposal = recoverProposal(msg);
        logger.info("[LA] - Processing proposal : " + proposal.toString());
        LatticeState state = slots.get(proposal.getSlot());
        if (state == null) {
            state = new LatticeState(proposal.getSlot(), proposal.getProposedValues());
            state.setActive(true);
            slots.put(proposal.getSlot(), state);
        }
        if (proposal.getProposedValues().containsAll(state.getAcceptedValues())) {
            state.getAcceptedValues().addAll(proposal.getProposedValues());
            Metadata metadata = new Metadata(MsgType.ACK, myHostIP.getId(), sender.getId(), 0, myHostIP, sender);
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(proposal.getActivePropose());
            buffer.putInt(proposal.getSlot());
            Message ack = new Message(metadata, buffer.array());
            logger.info("[LA] - Sending ACK for proposal : " + proposal.toString() + " to : " + sender.toString());
            pl.send(empack(ack), sender);
        } else {
            state.getAcceptedValues().addAll(proposal.getProposedValues());
            Metadata metadata = new Metadata(MsgType.NACK, myHostIP.getId(), sender.getId(), 0, myHostIP, sender);
            Message nack = new Message(metadata, prepareNackData(proposal, state.getAcceptedValues()));
            logger.info("[LA] - Sending NACK for proposal : " + proposal.toString() + " to : " + sender.toString());
            pl.send(empack(nack), sender);
        }
    }

    private void processAck(Message msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getData());
        int proposalNb = buffer.getInt();
        int slot = buffer.getInt();
        LatticeState state = slots.get(slot);
        if (state == null) {
            logger.info("[LA] - This state does not exist");
            return;
        }
        if (state.hasResponded(msg.getSenderHostIP())) {
            logger.info("[LA] - Already responded to this proposal");
            return;
        }
        logger.info("[LA] - Processing ACK : " + msg.getMetadata().toString() + " with proposal : "
                + proposalNb);
        if (proposalNb == state.getActivePropose()) {
            state.incrAckCount();
            state.addAck(msg.getSenderHostIP());
            logger.info("[LA] - incrementing ack count" + state.getAckCount() + " for slot " + slot);
            if (state.getAckCount() > destinations.size() / 2 && state.isActive()) {
                state.setActive(false);
                Proposal decidedProposal = new Proposal(state.getActivePropose(), state.getProposedValues(), slot);
                decide(decidedProposal);
            }
        }
    }

    private void processNack(Message msg) {
        Proposal proposal = recoverProposal(msg);
        logger.info("[LA] - Processing NACK : " + msg.getMetadata().toString() + " with proposal : "
                + proposal.toString());
        LatticeState state = slots.get(proposal.getSlot());
        if (state == null) {
            logger.info("[LA] - This state does not exist");
            return;
        }
        if (state.hasResponded(msg.getSenderHostIP())) {
            logger.info("[LA] - Already responded to this proposal");
            return;
        }
        if (proposal.getActivePropose() == state.getActivePropose()) {
            state.incrNackCount();
            state.addNack(msg.getSenderHostIP());
            state.getProposedValues().addAll(proposal.getProposedValues());
            if (state.getNackCount() + state.getAckCount() > destinations.size() / 2 && state.isActive()) {
                state.incrActivePropose();
                state.resetAckCount();
                state.resetNackCount();
                Proposal newProposal = new Proposal(state.getActivePropose(), state.getProposedValues(),
                        proposal.getSlot());
                Message m = prepareProposalMsg(newProposal);
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
