package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs451.Models.HostIP;
import cs451.Parser.Host;

/**
 * Class representing a host in the network.
 */
public class UDPHost implements Publisher<DatagramPacket> {

    private static final int MAX_PKT_SIZE = 65535;
    private DatagramSocket socket;
    private SubmissionPublisher<DatagramPacket> publisher;
    private final Logger logger = Logger.getLogger(UDPHost.class.getName());
    private HostIP hostIP;

    private AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService executor;

    /**
     * Creates a host with a given port number and IP address.
     * 
     * @param host     Host object containing port number, IP address and id.
     * @param executor ExecutorService to run threads.
     */
    public UDPHost(Host host, ExecutorService executor) {
        if (host.getPort() < 0 || host.getPort() > 65535) {
            System.err.println("Port number must be between 0 and 65535!");
            return;
        }

        InetAddress address;

        try {
            address = InetAddress.getByName(host.getIp());
        } catch (UnknownHostException e) {
            System.err.println("IP address is not valid!");
            running.set(false);
            return;
        } catch (SecurityException s) {
            System.err.println("Cannot resolve address, SecurityException");
            running.set(false);
            return;
        }

        try {
            socket = new DatagramSocket(host.getPort(), address);
        } catch (SocketException e) {
            e.printStackTrace();
            running.set(false);
            return;
        } catch (SecurityException s) {
            System.err.println("Cannot resolve Socket, SecurityException");
            running.set(false);
            return;
        }
        this.executor = executor;
        publisher = new SubmissionPublisher<>(executor, 256);
        hostIP = new HostIP(host);
        running.set(true);
        logger.setLevel(Level.OFF);
    }

    /**
     * Sends a packet to the host.
     * 
     * @param packet DatagramPacket to send.
     */
    public void send(DatagramPacket packet) {
        logger.log(Level.INFO,
                "Sending packet to " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                        + " with length "
                        + packet.getLength() + " and hashcode " + packet.hashCode());
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        return;
    }

    /**
     * Receives packets from the host.
     */
    public void receive() {
        executor.submit(() -> {
            while (running.get()) {
                byte[] buf = new byte[MAX_PKT_SIZE];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                    logger.warning("" + packet.getLength());
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                logger.log(Level.INFO,
                        "Received packet from " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                                + " with length "
                                + packet.getLength() + " and hashcode " + packet.hashCode());
                publisher.submit(packet);

            }
            return;
        });

    }

    /**
     * Stops the host.
     */
    public void stop() {
        running.set(false);
        socket.close();
    }

    /**
     * Returns the port number of the host.
     * 
     * @return Port number of the host.
     */
    public int getPort() {
        return socket.getLocalPort();
    }

    /**
     * Returns the IP address of the host.
     * 
     * @return IP address of the host.
     */
    public HostIP getHostIP() {
        return hostIP;
    }

    /**
     * Subscribes a subscriber to the publisher.
     */
    @Override
    public void subscribe(Subscriber<? super DatagramPacket> subscriber) {
        publisher.subscribe(subscriber);
    }

}
