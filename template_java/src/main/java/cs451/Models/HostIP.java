package cs451.Models;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import cs451.Parser.Host;

public class HostIP implements Serializable {
    private InetAddress address;
    private int port;
    private int id;

    public HostIP(Host host) {
        try {
            this.address = InetAddress.getByName(host.getIp());
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.port = host.getPort();
        this.id = host.getId();
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getId() {
        return id;
    }

    public String toString() {
        return address.getHostAddress().toString() + ":" + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof HostIP))
            return false;
        HostIP ip = (HostIP) obj;
        return this.address.equals(ip.getAddress()) && this.port == ip.getPort() && this.id == ip.getId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port, id);
    }

    public static Set<HostIP> fromHosts(List<Host> hosts) {
        Set<HostIP> ipAddresses = new HashSet<HostIP>();
        for (Host host : hosts) {
            HostIP ip = new HostIP(host);
            ipAddresses.add(ip);
        }
        return ipAddresses;

    }
}
