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

public class UDPHost implements Publisher<DatagramPacket> {

    private DatagramSocket socket;
    private SubmissionPublisher<DatagramPacket> publisher;
    private final Logger logger = Logger.getLogger(UDPHost.class.getName());

    private AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService executor;

    public UDPHost(int portNbr, String ip, ExecutorService executor) {
        if (portNbr < 0 || portNbr > 65535) {
            System.err.println("Port number must be between 0 and 65535!");
            return;
        }

        InetAddress address;

        try {
            address = InetAddress.getByName(ip);
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
            socket = new DatagramSocket(portNbr, address);
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
        running.set(true);
        logger.setLevel(Level.OFF);
    }

    /**
     * Sends a packet to the host.
     * 
     * @param packet DatagramPacket to send.
     * @return true if the packet was sent successfully, false otherwise.
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
     * Receives a packet from the host.
     * 
     * @return DatagramPacket received from the host. Returns false if an error
     *         occurs.
     */
    public void receive() {
        executor.submit(() -> {
            while (running.get()) {
                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
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

    public void stop() {
        running.set(false);
        socket.close();
    }

    public int getPort() {
        return socket.getLocalPort();
    }

    public InetAddress getAddress() {
        return socket.getLocalAddress();
    }

    @Override
    public void subscribe(Subscriber<? super DatagramPacket> subscriber) {
        publisher.subscribe(subscriber);
    }

}
