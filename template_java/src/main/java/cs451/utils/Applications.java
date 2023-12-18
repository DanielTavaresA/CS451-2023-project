package cs451.utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import cs451.Agreement.LatticeAgreement;
import cs451.Broadcast.BestEffortBroadcast;
import cs451.Broadcast.FIFOBroadcast;
import cs451.Broadcast.UniformReliableBroadcast;
import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.HostIP;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
import cs451.Models.Proposal;
import cs451.Parser.Host;
import cs451.Parser.Parser;

/**
 * This class contains methods to run different applications for the distributed
 * algorithm.
 */
public class Applications {

    /**
     * Runs the Perfect Link protocol.
     * 
     * @param parser the parser object containing the necessary information for the
     *               protocol
     */
    public static void runPerfectLinks(Parser parser) {
        List<Host> hosts = parser.hosts();

        Host myHost = hosts.get(parser.myId() - 1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UDPHost myUDPHost = new UDPHost(myHost, executor);
        myUDPHost.receive();
        HostIP myHostIP = myUDPHost.getHostIP();

        int[] config = readPerfectConfigFile(parser.config());

        Log.logPath = Paths.get(parser.output());

        int nbMsg = config[0];
        int recieverId = config[1];
        Host recieverHost = hosts.get(recieverId - 1);

        PerfectLink perfectLink = new PerfectLink(myUDPHost, executor);
        perfectLink.activateLogging();

        if (parser.myId() != recieverId) {
            for (int i = 1; i <= nbMsg; i++) {
                byte[] data = Integer.toString(i).getBytes();
                HostIP recieverIP = new HostIP(recieverHost);
                Metadata metadata = new Metadata(MsgType.DATA, parser.myId(), recieverId, 0, myHostIP,
                        recieverIP);
                Message msg = new Message(metadata, data);
                perfectLink.send(msg, recieverIP);
            }
        }

    }

    public static void runBebBroadcast(Parser parser) {
        List<Host> hosts = parser.hosts();

        Host myHost = hosts.get(parser.myId() - 1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UDPHost myUDPHost = new UDPHost(myHost, executor);
        myUDPHost.receive();
        HostIP myAddress = myUDPHost.getHostIP();

        int nbMsg = readFifoConfigFile(parser.config());

        Log.logPath = Paths.get(parser.output());

        Set<HostIP> destinations = HostIP.fromHosts(hosts);

        BestEffortBroadcast beb = new BestEffortBroadcast(myUDPHost, destinations, executor);
        for (int i = 1; i <= nbMsg; i++) {
            byte[] data = Integer.toString(i).getBytes();
            Metadata metadata = new Metadata(MsgType.DATA, parser.myId(), 0, 0, myAddress, null);
            Message msg = new Message(metadata, data);
            beb.broadcast(msg);
        }

    }

    public static void runFIFOBroadcast(Parser parser) {
        List<Host> hosts = parser.hosts();

        Host myHost = hosts.get(parser.myId() - 1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UDPHost myUDPHost = new UDPHost(myHost, executor);
        myUDPHost.receive();
        HostIP myHostIP = myUDPHost.getHostIP();

        int nbMsg = readFifoConfigFile(parser.config());

        Log.logPath = Paths.get(parser.output());

        Set<HostIP> destinations = HostIP.fromHosts(hosts);

        FIFOBroadcast fb = new FIFOBroadcast(myUDPHost, destinations, executor);
        fb.activateLogging();
        for (int i = 1; i <= nbMsg; i++) {
            Metadata metadata = new Metadata(MsgType.DATA, myHostIP.getId(), 0, i, myHostIP, null);
            Message msg = new Message(metadata, "broadcast".getBytes());
            fb.broadcast(msg);
        }

    }

    public static void runLatticeAgreement(Parser parser) {
        List<Host> hosts = parser.hosts();

        Host myHost = hosts.get(parser.myId() - 1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UDPHost myUDPHost = new UDPHost(myHost, executor);
        myUDPHost.receive();
        HostIP myHostIP = myUDPHost.getHostIP();

        String[] config = readLatticeConfig(parser.config());
        int[] latticeConfig = Stream.of(config[0].split(" ")).map(x -> x.strip()).mapToInt(Integer::parseInt).toArray();
        int p = latticeConfig[0];
        int vs = latticeConfig[1];
        int ds = latticeConfig[2];

        Log.logPath = Paths.get(parser.output());

        Set<HostIP> destinations = HostIP.fromHosts(hosts);

        LatticeAgreement la = new LatticeAgreement(myUDPHost, executor, destinations);
        for (int i = 1; i <= p; i++) {
            Set<Integer> proposedValues = Stream.of(config[i].split(" ")).map(x -> x.strip())
                    .mapToInt(Integer::parseInt)
                    .collect(HashSet::new,
                            HashSet::add, HashSet::addAll);
            Proposal proposal = new Proposal(0, proposedValues, i);
            la.propose(proposal);
        }

    }

    /**
     * Reads the configuration file at the given path and returns an array
     * containing the values of m and i.
     * The configuration file should have two space-separated integers on the first
     * line, representing m and i respectively.
     * If the file cannot be read or parsed, null is returned.
     *
     * @param path the path to the configuration file
     * @return an array containing the values of m and i, or null if the file cannot
     *         be read or parsed
     */
    private static int[] readPerfectConfigFile(String path) {
        try {
            String content = Files.readString(Paths.get(path));
            String[] entries = content.strip().split(" ");
            int m = Integer.parseInt(entries[0]);
            int i = Integer.parseInt(entries[1]);
            return new int[] { m, i };

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    private static int readFifoConfigFile(String path) {
        try {
            String content = Files.readString(Paths.get(path));
            String[] entries = content.strip().split(" ");
            int m = Integer.parseInt(entries[0]);
            return m;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }

    private static String[] readLatticeConfig(String path) {
        try {
            String content = Files.readString(Paths.get(path));
            String[] entries = content.strip().split("\n");
            return entries;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
