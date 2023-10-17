package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.Future;

import cs451.Models.Message;

/**
 * Class representing a link between two hosts.
 */
public interface Link {

    public abstract Future<Boolean> send(Message m, UDPHost host, InetAddress dest, int port);

    public abstract Future<DatagramPacket> deliver(UDPHost host);

}