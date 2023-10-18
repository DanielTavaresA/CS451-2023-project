package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cs451.Models.Message;

/*
 * Class implementing a fair loss link. 
 * Properties of fair loss links should be satisfied.
 * - FL1 : Fair-loss : If a message is sent infinitely often then m is deliered infinitely often
 * - FL2 : Finite duplication : If a message is sent a finite number of times then m is delivered a finite number of times
 * - FL3 : No creation : No message is delivered unless it was sent
 */
public class FairLossLink implements Link {

    @Override
    public CompletableFuture<Boolean> send(Message m, UDPHost host, InetAddress dest, int port) {
        DatagramPacket packet = new DatagramPacket(m.toBytes(), m.toBytes().length, dest, port);
        return host.send(packet);

    }

    @Override
    public CompletableFuture<DatagramPacket> deliver(UDPHost host) {
        return host.receive();
    }

}
