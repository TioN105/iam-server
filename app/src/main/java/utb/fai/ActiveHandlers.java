package utb.fai;

import java.util.*;

public class ActiveHandlers {
    private static final long serialVersionUID = 1L;
    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();

    /**
     * Posle zpravu vsem klientum, kteri sdili alespon jednu diskuzni mistnost s odesilatelem (krom odesilatele samotneho).
     */
    synchronized void sendMessageToGroup(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && handler.userName != null) {
                boolean sharesGroup = false;
                synchronized (sender.groups) {
                    for (String g : sender.groups) {
                        if (handler.groups.contains(g)) {
                            sharesGroup = true;
                            break;
                        }
                    }
                }
                if (sharesGroup) {
                    if (!handler.messages.offer(message)) {
                        System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                    }
                }
            }
        }
    }

    /**
     * Posle soukromou zpravu konkretnimu uzivateli podle jmena.
     */
    synchronized void sendPrivateMessage(SocketHandler sender, String recipientName, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler.userName != null && handler.userName.equals(recipientName)) {
                String formatted = "[" + sender.userName + "] >> " + message;
                if (!handler.messages.offer(formatted)) {
                    System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                }
                return;
            }
        }
    }

    /**
     * Zjisti, zda je zadane jmeno uz pouzivano jinym klientem.
     */
    synchronized boolean isNameTaken(SocketHandler sender, String name) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && handler.userName != null && handler.userName.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }
}