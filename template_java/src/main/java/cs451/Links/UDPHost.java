package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class UDPHost {

    private DatagramSocket socket;
    private boolean running;

    public UDPHost(int portNbr) {
        if (portNbr < 0 || portNbr > 65535) {
            System.err.println("Port number must be between 0 and 65535!");
            return;
        }
        try {
            socket = new DatagramSocket(portNbr);
        } catch (SocketException e) {
            e.printStackTrace();
            running = false;
            return;
        }
        running = true;
    }

    public boolean send(DatagramPacket packet) {
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
            socket.receive(packet);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
