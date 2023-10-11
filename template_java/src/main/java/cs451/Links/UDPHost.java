package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class UDPHost {

    private DatagramSocket socket;
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
        } catch (SecurityException s){
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
        }
        catch (SecurityException s){
            System.err.println("Cannot resolve Socket, SecurityException");
            running = false;
            return;
        }
        running = true;
    }

    public boolean send(DatagramPacket packet) {
        System.out.println(
                "Sending packet to " + packet.getAddress().getHostAddress() + ":" + packet.getPort() + " with length "
                        + packet.getLength() + " and data " + new String(packet.getData()));
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean receive() {
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        try {
            socket.setSoTimeout(5000);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        try {
            socket.receive(packet);

            System.out.println("Received packet from " + packet.getAddress().getHostAddress() + ":" + packet.getPort()
                    + " with length "
                    + packet.getLength() + " and data " + new String(packet.getData()));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
