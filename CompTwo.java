import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CompTwo {
    private ServerSocket serverSocket3;
    private ServerSocket serverSocket4;
    private Socket client;
    private int connectedClientsCount;
    private int allConnectedClientsCount;

    public void start() {
        try{
            System.out.println("Client: Connecting to CompOne: 10.176.69.32  on port 9949." );
            // connecting to CompOne.
            client = new Socket("10.176.69.32", 9949);
            System.out.println("Client: Just connected to " + client.getRemoteSocketAddress());
            connectedClientsCount++;
            serverSocket3 = new ServerSocket(9952);
            System.out.println("Server is running on port " + 9952);
            // connecting to CompThree.
            final Socket clientSocket3 = serverSocket3.accept(); 
            connectedClientsCount++;
            System.out.println("Client: Just connected to " + clientSocket3.getRemoteSocketAddress());
            serverSocket4 = new ServerSocket(9953);
            System.out.println("Server is running on port " + 9953);
            // connecting to CompFour.
            final Socket clientSocket4 = serverSocket4.accept();
            connectedClientsCount++;
            System.out.println("Client: Just connected to " + clientSocket4.getRemoteSocketAddress());
            System.out.println("Client's connected. Total connected clients: " + connectedClientsCount);
            if(connectedClientsCount == 3) {
                allConnectedClientsCount++;
            }
            DataOutputStream out3 = new DataOutputStream(clientSocket3.getOutputStream());
            out3.writeUTF("compTwo ready");
            DataOutputStream out1 = new DataOutputStream(client.getOutputStream());
            out1.writeUTF("compTwo ready");
            DataOutputStream out4 = new DataOutputStream(clientSocket4.getOutputStream());
            out4.writeUTF("compTwo ready");
            System.out.println("Client: Sent ready message to CompOne and CompThree.");
            // waiting for the message from CompOne.
            Thread t1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in1 = new DataInputStream(client.getInputStream());
                        while(true) {
                            String message = in1.readUTF();
                            if(message.equals("compOne ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("Client: IO exception occured in thread t1.");
                    }
                }
            });
            Thread t2 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in3 = new DataInputStream(clientSocket3.getInputStream());
                        while(true) {
                            String message = in3.readUTF();
                            if(message.equals("compThree ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("Client: IO exception occured in thread t2.");
                    }
                }
            });
            Thread t3 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in4 = new DataInputStream(clientSocket4.getInputStream());
                        while(true) {
                            String message = in4.readUTF();
                            if(message.equals("compFour ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("Client: IO exception occured in thread t3.");
                    }
                }
            });
            t1.start();
            t2.start();
            t3.start();
            try {
                t1.join();
                t2.join();
                t3.join();
            } catch (InterruptedException e) {
                System.out.println("Client: Interrupted exception occured in start function.");
            }
            System.out.println("Client: All clients are connected. Total connected clients: " + allConnectedClientsCount);
            if(allConnectedClientsCount == 4) {
                new CompTwoClientHandler(client, clientSocket3, clientSocket4).start();
            }
        } catch (IOException e) {
            System.out.println("Client: IO exception occured in start function.");
        }
    }

    private class CompTwoClientHandler extends Thread {
        private final Socket client1;
        private final Socket clientSocket3;
        private final Socket clientSocket4;
        final ReentrantLock vectorClock_lock = new ReentrantLock();
        public CompTwoClientHandler(Socket client, final Socket clientSocket3, final Socket clientSocket4) {
            this.client1 = client;
            this.clientSocket3 = clientSocket3;
            this.clientSocket4 = clientSocket4;
        }

        private class Message {
            private int processName;
            private int messageNumber;
            private List<Integer> timeStamp;
            private String message;
            public Message(int processName, int messageNumber, List<Integer> timeStamp, String message) {
                this.processName = processName;
                this.messageNumber = messageNumber;
                this.timeStamp = timeStamp;
                this.message = message;
            }
        }

        public Message createMessage(String msg) throws IOException {
            // based on the incoming message form the Message object
            // split the message based on the delimiter and create a Message object
            String in = msg.replaceAll("[\\[\\]\" ]", "");
            String[] msgArray = in.split(",");
            int processName = Integer.parseInt(msgArray[0]);
            int messageNumber = Integer.parseInt(msgArray[1].split(":")[1]);
            List<Integer> vectorClock = new ArrayList<Integer>();
            for(int i=2; i<msgArray.length; i++) {
                String s = msgArray[i];
                vectorClock.add(Integer.parseInt(s));
            }
            String message = msgArray[1];
            return new Message(processName, messageNumber, vectorClock, message);
        }

        public synchronized void flushBuffer(List<Message> buffer, List<Integer> vectorClock, Message msg, int currProcessName) {
            boolean canFlush = true;
            while(canFlush) {
                canFlush = false;
                for(int i = buffer.size()-1; i >=0 ; i--) {
                    Message m = buffer.get(i);
                    // if for a messsage, the process from which the message is received that bit should only be one bit higher and not more greater than the vector clock that bit
                    if(m.timeStamp.get(m.processName - 1) != vectorClock.get(m.processName - 1) + 1) {
                        // keep it in buffer and move for the next message
                        continue;
                    }
                    //except the current process bit and the process from which the message is received bit, then for the rest of the bits if the bit is greater than the vector clock bit 
                    // then keep it in the buffer and move for the next message
                    boolean isValid = true;
                    for(int j = 0; j < vectorClock.size(); j++) {
                        if(j != currProcessName-1 && j != m.processName - 1) {
                            if(m.timeStamp.get(j) > vectorClock.get(j)) {
                                isValid = false;
                                break;
                            }
                        }
                    }
                    if(!isValid) continue;
                    // else the msg is eligible for delivery and update the vector clock of the process and remove it from the buffer and print the message and set the flag to true
                    // check the component wise max and update the vector
                    for(int j = 0; j < vectorClock.size(); j++) {
                        vectorClock.set(j, Math.max(vectorClock.get(j), m.timeStamp.get(j)));
                    }
                    System.out.println("Process - "+m.processName + " : " +m.messageNumber+")" + m.message+" Delivered." );
                    buffer.remove(i);
                    canFlush = true;
                    break;
                }
            }
        }

        public void run() {
            try {
                // set the initial vector clock as list of 3 elements with all elements as 0
                final List<Integer> vectorClock = new ArrayList<Integer>(Arrays.asList(0, 0, 0, 0));
                // set the processname as 2.
                final int processName = 2;
                final List<Message> buffer = new ArrayList<Message>();
                ReadWriteLock lock = new ReentrantReadWriteLock();
                final Lock writeLock = lock.writeLock();
                // create the thread's to receive the messages from compOne and compThree and compFour and send messages
                Thread t1 = new Thread(new Runnable() {
                    public void run() {
                        try {
                            DataInputStream in1 = new DataInputStream(client1.getInputStream());
                            while(true) {
                                String message1 = in1.readUTF();
                                if(message1.equals("exit")) {
                                    break;
                                }
                                Message msg = createMessage(message1);

                                // emulating network delay at buffer

                                //Wait for a random amount of time in the range(0,5] milliseconds.
                                // try {
                                //     Thread.sleep((long)(Math.random() * 5));
                                // } catch (InterruptedException e) {
                                //     System.out.println("Thread interrupted.");
                                // }
                                writeLock.lock();
                                try {
                                    buffer.add(msg);
                                } finally {
                                    writeLock.unlock();
                                }
                                vectorClock_lock.lock();
                                try {
                                    flushBuffer(buffer, vectorClock, msg, processName);
                                } finally {
                                    vectorClock_lock.unlock();
                                }
                            }
                        } catch (IOException e) {
                            System.out.println("Client: IO exception occured in thread t1.");
                        }
                    }
                });

                Thread t2 = new Thread(new Runnable() {
                    public void run() {
                        try {
                            DataInputStream in3 = new DataInputStream(clientSocket3.getInputStream());
                            while(true) {
                                String message2 = in3.readUTF();
                                if(message2.equals("exit")) {
                                    break;
                                }
                                Message msg = createMessage(message2);

                                // emulating network delay at buffer

                                //Wait for a random amount of time in the range(0,5] milliseconds.
                                // try {
                                //     Thread.sleep((long)(Math.random() * 5));
                                // } catch (InterruptedException e) {
                                //     System.out.println("Thread interrupted.");
                                // }
                                writeLock.lock();
                                try {
                                    buffer.add(msg);
                                } finally {
                                    writeLock.unlock();
                                }
                                vectorClock_lock.lock();
                                try {
                                    flushBuffer(buffer, vectorClock, msg, processName);
                                } finally {
                                    vectorClock_lock.unlock();
                                }
                            }
                        } catch (IOException e) {
                            System.out.println("Client: IO exception occured in thread t1.");
                        }
                    }
                });

                Thread t3 = new Thread(new Runnable() {
                    public void run() {
                        try {
                            DataInputStream in4 = new DataInputStream(clientSocket4.getInputStream());
                            while(true) {
                                String message3 = in4.readUTF();
                                if(message3.equals("exit")) {
                                    break;
                                }
                                Message msg = createMessage(message3);

                                // emulating network delay at buffer

                                //Wait for a random amount of time in the range(0,5] milliseconds.
                                // try {
                                //     Thread.sleep((long)(Math.random() * 5));
                                // } catch (InterruptedException e) {
                                //     System.out.println("Thread interrupted.");
                                // }
                                writeLock.lock();
                                try {
                                    buffer.add(msg);
                                } finally {
                                    writeLock.unlock();
                                }
                                vectorClock_lock.lock();
                                try {
                                    flushBuffer(buffer, vectorClock, msg, processName);
                                } finally {
                                    vectorClock_lock.unlock();
                                }
                            }
                        } catch (IOException e) {
                            System.out.println("Client: IO exception occured in thread t1.");
                        }
                    }
                });

                Thread t4 = new Thread(new Runnable() {
                    public void run() {
                        try {
                            DataOutputStream out1 = new DataOutputStream(client1.getOutputStream());
                            DataOutputStream out3 = new DataOutputStream(clientSocket3.getOutputStream());
                            DataOutputStream out4 = new DataOutputStream(clientSocket4.getOutputStream());
                            for(int i = 1; i <= 100; i++) {
                                // increment the vector clock of the process by 1 for the second bit every time a message is sent
                                vectorClock.set(processName - 1, vectorClock.get(processName - 1) + 1);
                                // convert the vector clock to string and send it as part of the message
                                String vectorClockString = vectorClock.toString();
                                // format the messages as "1,Message:1,100", "1,Message:2,200" as "from, message, timestamp".
                                String msgToSent = processName+","+"Message:"+i+","+vectorClockString;
                                try {
                                    Thread.sleep((long)(Math.random() * 10));
                                } catch (InterruptedException e) {
                                    System.out.println("Thread interrupted.");
                                }
                                out1.writeUTF(msgToSent);
                                out1.flush();
                                out3.writeUTF(msgToSent);
                                out3.flush();
                                out4.writeUTF(msgToSent);
                                out4.flush();
                            }
                            out1.writeUTF("exit");
                            out1.flush();
                            out3.writeUTF("exit");
                            out3.flush();
                            out4.writeUTF("exit");
                            out4.flush();
                        } catch (IOException e) {
                            System.out.println("Client: IO exception occured in thread t2.");
                        }
                    }
                });
                t1.start();
                t2.start();
                t3.start();
                t4.start();
                try {
                    t1.join();
                    t2.join();
                    t3.join();
                    t4.join();
                } catch (InterruptedException e) {
                    System.out.println("Client: Interrupted exception occured in CompTwoClientHandler.");
                }
                System.out.println("Client: All threads are finished.");
                client1.close();
                clientSocket3.close();
            } catch (IOException e) {
                System.out.println("Client: IO exception occured in CompTwoClientHandler.");
            }
        }
    }

    public static void main(String[] args) {
        CompTwo compTwo = new CompTwo();
        compTwo.start();
    }
}