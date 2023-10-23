package cs451.Links;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import cs451.Models.Message;

/*
 * Class implementing a fair loss link. 
 * Properties of fair loss links should be satisfied.
 * - FL1 : Fair-loss : If a message is sent infinitely often then m is deliered infinitely often
 * - FL2 : Finite duplication : If a message is sent a finite number of times then m is delivered a finite number of times
 * - FL3 : No creation : No message is delivered unless it was sent
 */
public class FairLossLink implements Link, Subscriber<DatagramPacket> {

    private Subscription subscription;

    public FairLossLink(UDPHost host) {
        host.subscribe(this);
        
    }


    @Override
    public CompletableFuture<Boolean> send(Message m, UDPHost host, InetAddress dest, int port) {
        DatagramPacket packet = new DatagramPacket(m.toBytes(), m.toBytes().length, dest, port);
        System.out.println("[FLL] - Sending message : " + m.getId() + " to " + dest.getHostAddress() + ":" + port);
        return host.send(packet);

    }

    @Override
    public CompletableFuture<Boolean> deliver(DatagramPacket packet) {
        Message msg = Message.fromBytes(packet.getData());
        System.out.println("[FLL] - Delivering message : " + msg.getId() + " from " + packet.getAddress().getHostAddress() + ":" + packet.getPort());
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);

    }

    @Override
    public void onNext(DatagramPacket item) {
        deliver(item);
        subscription.request(1);
        
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("Completed");
    }


}
