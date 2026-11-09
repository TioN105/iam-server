package utb.fai;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class SocketHandler {
    Socket mySocket;
    String clientID;
    volatile String userName = null;
    Set<String> groups = Collections.synchronizedSet(new LinkedHashSet<String>());
    ActiveHandlers activeHandlers;

    ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);
    CountDownLatch startSignal = new CountDownLatch(2);

    OutputHandler outputHandler = new OutputHandler();
    InputHandler inputHandler = new InputHandler();

    volatile boolean inputFinished = false;

    public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
        this.mySocket = mySocket;
        clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
        this.activeHandlers = activeHandlers;
    }

    class OutputHandler implements Runnable {
        public void run() {
            OutputStreamWriter writer;
            try {
                System.err.println("DBG>Output handler starting for " + clientID);
                startSignal.countDown();
                startSignal.await();
                System.err.println("DBG>Output handler running for " + clientID);
                
                writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
                
                writer.write("Enter your name:\r\n");
                writer.flush();

                while (!inputFinished) {
                    String m = messages.take();
                    if (inputFinished) {
                        break;
                    }
                    writer.write(m + "\r\n");
                    writer.flush();
                    System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
                }
            } catch (IOException e) {
            } catch (InterruptedException e) {
            }
            System.err.println("DBG>Output handler for " + clientID + " has finished.");
        }
    }

    class InputHandler implements Runnable {
        public void run() {
            try {
                System.err.println("DBG>Input handler starting for " + clientID);
                startSignal.countDown();
                startSignal.await();
                System.err.println("DBG>Input handler running for " + clientID);

                activeHandlers.add(SocketHandler.this);
                BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
                String request = "";

                while ((request = reader.readLine()) != null) {
                    request = request.trim();
                    if (request.isEmpty()) {
                        continue;
                    }

                    // 1. Nastaveni jmena pri prvnim pripojeni
                    if (userName == null) {
                        String name = request;
                        if (name.startsWith("#setMyName")) {
                            name = name.substring("#setMyName".length()).trim();
                        }

                        if (name.isEmpty() || name.contains(" ") || name.contains("\t") || activeHandlers.isNameTaken(SocketHandler.this, name)) {
                            messages.offer("Invalid or taken name. Enter name:");
                        } else {
                            userName = name;
                            groups.add("public");
                        }
                        continue;
                    }

                    // 2. Prikazy
                    if (request.startsWith("#setMyName")) {
                        String newName = request.substring("#setMyName".length()).trim();
                        if (!newName.isEmpty() && !newName.contains(" ") && !newName.contains("\t")) {
                            if (!activeHandlers.isNameTaken(SocketHandler.this, newName)) {
                                userName = newName;
                            }
                        }
                    } else if (request.startsWith("#sendPrivate")) {
                        String body = request.substring("#sendPrivate".length()).trim();
                        String[] parts = body.split("\\s+", 2);
                        if (parts.length == 2) {
                            String targetName = parts[0];
                            String msgText = parts[1];
                            activeHandlers.sendPrivateMessage(SocketHandler.this, targetName, msgText);
                        }
                    } else if (request.startsWith("#join")) {
                        String room = request.substring("#join".length()).trim();
                        if (!room.isEmpty() && !room.contains(" ")) {
                            groups.add(room);
                        }
                    } else if (request.startsWith("#leave")) {
                        String room = request.substring("#leave".length()).trim();
                        if (!room.isEmpty()) {
                            groups.remove(room);
                        }
                    } else if (request.equalsIgnoreCase("#groups")) {
                        synchronized (groups) {
                            String groupList = String.join(",", groups);
                            messages.offer(groupList);
                        }
                    } else if (!request.startsWith("#")) {
                        String formattedMsg = "[" + userName + "] >> " + request;
                        System.out.println("From " + userName + " (" + clientID + "): " + request);
                        activeHandlers.sendMessageToGroup(SocketHandler.this, formattedMsg);
                    }
                }
                inputFinished = true;
                messages.offer("OutputHandler, wakeup and die!");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                activeHandlers.remove(SocketHandler.this);
            }
            System.err.println("DBG>Input handler for " + clientID + " has finished.");
        }
    }
}