package cs451.Broadcast;

import java.net.DatagramPacket;

import cs451.Models.Message;

public interface Broadcaster {

    void broadcast(Message m);

    void deliver(DatagramPacket pkt);

}
