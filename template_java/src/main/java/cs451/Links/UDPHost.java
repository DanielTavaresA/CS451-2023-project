package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class UDPHost {

    private DatagramSocket socket;
    ExecutorService executor = Executors.newCachedThreadPool();
    private boolean running;

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
            running = false;
            return;
        } catch (SecurityException s) {
            System.err.println("Cannot resolve address, SecurityException");
            running = false;
            return;
        }

        try {
            socket = new DatagramSocket(portNbr, address);
        } catch (SocketException e) {
            e.printStackTrace();
            running = false;
            return;
        } catch (SecurityException s) {
            System.err.println("Cannot resolve Socket, SecurityException");
            running = false;
            return;
        }
        running = true;
    }

    /**
     * Sends a packet to the host. Sets timeout to 5 seconds.
     * 
     * @param packet DatagramPacket to send.
     * @return true if the packet was sent successfully, false otherwise.
     */
    public Future<Boolean> send(DatagramPacket packet) {
        return executor.submit(() -> {
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
     * Receives a packet from the host. Sets timeout to 5 seconds.
     * 
     * @return DatagramPacket received from the host. Returns null if an error
     *         occurs.
     */
    public Future<DatagramPacket> receive() {
        return executor.submit(() -> {
            byte[] buf = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            System.out.println(
                    "Received packet from " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                            + " with length "
                            + packet.getLength() + " and hashcode " + packet.hashCode());
            return packet;
        });

    }

    public int getPort() {
        return socket.getPort();
    }

    public InetAddress getAddress() {
        return socket.getInetAddress();
    }

}
