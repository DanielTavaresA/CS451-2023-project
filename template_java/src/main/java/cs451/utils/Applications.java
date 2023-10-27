package cs451.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import cs451.Links.PerfectLink;
import cs451.Links.UDPHost;
import cs451.Models.Message;
import cs451.Models.MsgType;
import cs451.Parser.Host;
import cs451.Parser.Parser;

public class Applications {

    public static void runPerfectLinks(Parser parser) {
        List<Host> hosts = parser.hosts();

        Host myHost = hosts.get(parser.myId() - 1);
        UDPHost myUDPHost = new UDPHost(myHost.getPort(), myHost.getIp());
        myUDPHost.receive();

        int[] config = readConfigFile(parser.config());

        Log.logPath = Paths.get(parser.output());

        int nbMsg = config[0];
        int recieverId = config[1];
        Host recieverHost = hosts.get(recieverId - 1);

        PerfectLink perfectLink = new PerfectLink(myUDPHost);

        if (parser.myId() != recieverId) {
            for (int i = 0; i < nbMsg; i++) {
                byte[] data = Integer.toString(i).getBytes();
                Message msg = new Message(MsgType.DATA, parser.myId(), recieverId, data);
                try {
                    InetAddress recieverIp = InetAddress.getByName(recieverHost.getIp());
                    perfectLink.send(msg, myUDPHost, recieverIp, recieverHost.getPort());
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    continue;
                }

            }
        }

    }

    private static int[] readConfigFile(String path) {
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
