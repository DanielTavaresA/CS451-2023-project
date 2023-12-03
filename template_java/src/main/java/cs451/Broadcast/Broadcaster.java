package cs451.Broadcast;

import cs451.Models.Message;

/**
 * Reprensent a broadcaster
 */
public interface Broadcaster {

    void broadcast(Message m);

    void deliver(Message msg);

}
