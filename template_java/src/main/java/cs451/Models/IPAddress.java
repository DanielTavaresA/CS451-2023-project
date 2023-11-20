package cs451.Models;

import java.io.Serializable;
import java.net.InetAddress;

public class IPAddress implements Serializable {
    private InetAddress address;
    private int port;

    public IPAddress(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return address.getHostAddress().toString() + ":" + port;
    }
}
