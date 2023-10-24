package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;

import cs451.Models.Message;

/**
 * Class representing a link between two hosts.
 */
public interface Link {

    public abstract void send(Message m, UDPHost host, InetAddress dest, int port);

    public abstract void deliver(DatagramPacket packet);

}