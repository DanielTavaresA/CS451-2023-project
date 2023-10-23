package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;

public class UDPHost implements Publisher<DatagramPacket>{

    private DatagramSocket socket;
    private final SubmissionPublisher<DatagramPacket> publisher = new SubmissionPublisher<>();
    
    private AtomicBoolean running = new AtomicBoolean(false);

    public UDPHost(int portNbr, String ip) {
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
        running.set(true);;
    }

    /**
     * Sends a packet to the host.
     * 
     * @param packet DatagramPacket to send.
     * @return true if the packet was sent successfully, false otherwise.
     */
    public CompletableFuture<Boolean> send(DatagramPacket packet) {
        return CompletableFuture.supplyAsync(() -> {
            System.out.println(
                    "Sending packet to " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                            + " with length "
                            + packet.getLength() + " and hashcode " + packet.hashCode());
            try {
                socket.send(packet);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        });
    }

    /**
     * Receives a packet from the host.
     * 
     * @return DatagramPacket received from the host. Returns false if an error
     *         occurs.
     */
    public CompletableFuture<Boolean> receive() {
        return CompletableFuture.supplyAsync(() -> {
            while(running.get()){
                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            System.out.println(
                    "Received packet from " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                            + " with length "
                            + packet.getLength() + " and hashcode " + packet.hashCode());
            publisher.submit(packet);
                
            }
            return true;
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
