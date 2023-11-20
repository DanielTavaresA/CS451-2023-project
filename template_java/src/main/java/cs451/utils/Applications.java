package cs451.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.IPAddress;
import cs451.Models.Message;
import cs451.Models.Metadata;
import cs451.Models.MsgType;
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
        UDPHost myUDPHost = new UDPHost(myHost.getPort(), myHost.getIp(), executor);
        myUDPHost.receive();
        IPAddress myAddress = new IPAddress(myUDPHost.getAddress(), myUDPHost.getPort());

        int[] config = readPerfectConfigFile(parser.config());

        Log.logPath = Paths.get(parser.output());

        int nbMsg = config[0];
        int recieverId = config[1];
        Host recieverHost = hosts.get(recieverId - 1);

        PerfectLink perfectLink = new PerfectLink(myUDPHost, executor);

        if (parser.myId() != recieverId) {
            for (int i = 1; i <= nbMsg; i++) {
                byte[] data = Integer.toString(i).getBytes();

                try {
                    InetAddress recieverIp = InetAddress.getByName(recieverHost.getIp());
                    IPAddress recieverAddress = new IPAddress(recieverIp, recieverHost.getPort());
                    Metadata metadata = new Metadata(MsgType.DATA, parser.myId(), recieverId, 0, myAddress,
                            recieverAddress);
                    Message msg = new Message(metadata, data);
                    perfectLink.send(msg, recieverAddress);
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    continue;
                }
            }
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

}
