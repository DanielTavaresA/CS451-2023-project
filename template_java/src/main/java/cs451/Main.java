package cs451;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import cs451.Links.FairLossLink;
import cs451.Links.PerfectLink;
import cs451.Links.StubbornLink;
import cs451.Links.UDPHost;
import cs451.Models.Message;
import cs451.Models.MsgType;
import cs451.Parser.Host;
import cs451.Parser.Parser;

public class Main {

    private static void handleSignal() {
        // immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        // write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid
                + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host : parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        System.out.println("Broadcasting and delivering messages...\n");

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.

        // Initiates a UDPHost on the port and IP of the current host
        List<Host> hosts = parser.hosts();
        Host myHost = hosts.get(parser.myId() - 1);
        UDPHost myUDPHost = new UDPHost(myHost.getPort(), myHost.getIp());
        myUDPHost.receive();
        /*
         * FairLossLink fairLossLink = new FairLossLink(myUDPHost);
         * 
         * Thread.sleep(5000);
         * 
         * /*
         * // Broadcast
         * for (Host host : hosts) {
         * if (host.getId() == parser.myId()) {
         * continue;
         * }
         * PerfectLink perfLink = new PerfectLink(myUDPHost, host.getPort(),
         * host.getIp());
         * perfLink.start();
         * }
         */

        // Send - Recieve Fair Loss

        /*
         * for (Host host : hosts) {
         * // if we are the host, send to all other hosts
         * if (host.getId() == parser.myId()) {
         * for (Host dest : hosts) {
         * if (dest.getId() == parser.myId()) {
         * continue;
         * }
         * InetAddress destAddress;
         * try {
         * destAddress = InetAddress.getByName(dest.getIp());
         * } catch (UnknownHostException e) {
         * e.printStackTrace();
         * continue;
         * }
         * Message m = new Message(MsgType.DATA, 0, "Hello World".getBytes());
         * fairLossLink.send(m, myUDPHost, destAddress, dest.getPort());
         * }
         * continue;
         * }
         * }
         * Thread.sleep(10000);
         * 
         * System.out.println("Done Fair loss");
         * 
         */

        // Send - Recieve StubbornLink

        PerfectLink perfLink = new PerfectLink(myUDPHost);
        for (Host host : hosts) {
            // if we are the host, send to all other hosts
            if (host.getId() == parser.myId()) {
                for (Host dest : hosts) {
                    if (dest.getId() == parser.myId()) {
                        continue;
                    }
                    InetAddress destAddress;
                    try {
                        destAddress = InetAddress.getByName(dest.getIp());
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        continue;
                    }
                    Message m = new Message(MsgType.DATA, 0, "Hello World".getBytes());
                    perfLink.send(m, myUDPHost, destAddress, dest.getPort());
                    System.out.println("Does not wait");
                }
                continue;
            }
        }

        System.out.println("Done Stubborn");
        System.out.println("Done, go sleep for 1 hour");

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
