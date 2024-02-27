import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CompOne {
    private ServerSocket serverSocket1;
    private ServerSocket serverSocket2;
    private ServerSocket serverSocket3;
    private int connectedClientsCount;
    private int allConnectedClientsCount;

    public void start() {
        try {
            serverSocket1 = new ServerSocket(9949);
            System.out.println("Server is running on port " + 9949);
            serverSocket2 = new ServerSocket(9950);
            System.out.println("Server is running on port " + 9950);

            // Adding fourth server
            serverSocket3 = new ServerSocket(9951);
            System.out.println("Server is running on port " + 9951);
            
           
            Socket clientSocket2 = serverSocket1.accept();
            connectedClientsCount++;
            Socket clientSocket3 = serverSocket2.accept();
            connectedClientsCount++;
            Socket clientSocket4 = serverSocket3.accept();
            connectedClientsCount++;
            System.out.println("Client's connected. Total connected clients: " + connectedClientsCount);
            if(connectedClientsCount == 3) {
                allConnectedClientsCount++;
            }
            DataOutputStream out1 = new DataOutputStream(clientSocket2.getOutputStream());
            out1.writeUTF("compOne ready");
            DataOutputStream out2 = new DataOutputStream(clientSocket3.getOutputStream());
            out2.writeUTF("compOne ready");
            DataOutputStream out3 = new DataOutputStream(clientSocket4.getOutputStream());
            out3.writeUTF("compOne ready");
            // wait for the messages from clientSocket2, clientSocket3 and clientSocket4 that they are connected to all in the clique and then start the process of sending messages all at once
            // create three paralleel threads and listen for compTwo ready and compThree ready messages
            // if all the messages are received then start the process of sending messages
            // if not then keep on waiting for the completion message
            Thread t1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in = new DataInputStream(clientSocket2.getInputStream());
                        while(true) {
                            String message = in.readUTF();
                            if(message.equals("compTwo ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("IO exception occured in receiving messages while checking status of compTwo.");
                    }
                }
            });
            Thread t2 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in = new DataInputStream(clientSocket3.getInputStream());
                        while(true) {
                            String message = in.readUTF();
                            if(message.equals("compThree ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("IO exception occured in receiving messages while checking status of compThree.");
                    }
                }
            });
            Thread t3 = new Thread(new Runnable() {
                public void run() {
                    try {
                        DataInputStream in = new DataInputStream(clientSocket4.getInputStream());
                        while(true) {
                            String message = in.readUTF();
                            if(message.equals("compFour ready")) {
                                allConnectedClientsCount++;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("IO exception occured in receiving messages while checking status of compFour.");
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
                System.out.println("Thread interrupted.");
             }
            if(allConnectedClientsCount == 4) {
                new CompOneClientHandler(clientSocket2, clientSocket3, clientSocket4).start();
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class CompOneClientHandler extends Thread {
        final private Socket clientSocket2;
        final private Socket clientSocket3;
        final private Socket clientSocket4;
        final ReentrantLock vectorClock_lock = new ReentrantLock();
        // to log the buffered messages to a log.txt file
        final String filePath = "logs.txt";
        public CompOneClientHandler(Socket socket1, Socket socket2, Socket socket3) {
            this.clientSocket2 = socket1;
            this.clientSocket3 = socket2;
            this.clientSocket4 = socket3;
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

        private synchronized void writeLog(String logMessage) {
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
                // Append log message to the file
                writer.println(logMessage);
            } catch (IOException e) {
                System.err.println("Error writing log to the file: " + e.getMessage());
            }
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

                        //Below lines are commented and can be used for logging purpose. 

                        // String l1 = "===================================================================================";
                        // String logMessage1 = "curr vec clock: "+vectorClock.toString()+" in process 1.";
                        // String logMessage2 = "verifying vec clock: "+ m.timeStamp.toString()+ " from the process "+m.processName;
                        // String l2 = "===================================================================================";
                        // writeLog(l1);
                        // writeLog(logMessage1);
                        // writeLog(logMessage2);
                        // writeLog(l2);
                        continue;
                    }
                    //except the current process bit and the process from which the message is received bit, then for the rest of the bits if the bit is greater than the vector clock bit 
                    // then keep it in the buffer and move for the next message
                    boolean isValid = true;
                    for(int j = 0; j < vectorClock.size(); j++) {
                        if(j != currProcessName-1 && j != m.processName - 1) {
                            if(m.timeStamp.get(j) > vectorClock.get(j)) {
                                //Below lines are commented and can be used for logging purpose.

                                // String l1 = "===================================================================================";
                                // String logMessage1 = "curr vec clock: "+vectorClock.toString()+" in process 1.";
                                // String logMessage2 = "verifying vec clock: "+ m.timeStamp.toString()+ " from the process "+m.processName;
                                // String l2 = "===================================================================================";
                                // writeLog(l1);
                                // writeLog(logMessage1);
                                // writeLog(logMessage2);
                                // writeLog(l2);
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
                // set the processname as 1.
                final int processName = 1;
                // create a buffer which is an arraylist to store the messages received and processed and delivered later
                final List<Message> buffer = new ArrayList<Message>();
                ReadWriteLock  lock = new ReentrantReadWriteLock();
                final Lock writeLock = lock.writeLock();
                // create the thread's to receive the messages from compTwo and compThree and compFour and send messages
                Thread t1 = new Thread(new Runnable() {
                    public void run() {
                        while(true) {
                            try {
                                DataInputStream two_in = new DataInputStream(clientSocket2.getInputStream());
                                String message1 = two_in.readUTF();
                                if(message1.equals("exit")) {
                                    break;
                                }

                                Message msg = createMessage(message1);
                                // logging purpose
                                // System.out.println(msg.message+" has arrived fom process 2.");

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
                            } catch (IOException e) {
                                System.out.println("IO exception occured in receiving messages.");
                                break;
                            }
                        }
                    }
                });

                Thread t2 = new Thread(new Runnable() {
                    public void run() {
                        while(true) {
                            try {
                                DataInputStream three_in = new DataInputStream(clientSocket3.getInputStream());
                                String message2 = three_in.readUTF();
                                // print the message received from compThree
                                if(message2.equals("exit")) {
                                    break;
                                }
                                Message msg = createMessage(message2);
                                //logging purpose
                                // System.out.println(msg.message+" has arrived fom process 3.");

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
                            } catch (IOException e) {
                                System.out.println("IO exception occured in receiving messages.");
                                break;
                            }
                        }
                    }
                });

                Thread t3 = new Thread(new Runnable() {
                    public void run() {
                        while(true) {
                            try {
                                DataInputStream four_in = new DataInputStream(clientSocket4.getInputStream());
                                String message3 = four_in.readUTF();
                                // print the message received from compThree
                                if(message3.equals("exit")) {
                                    break;
                                }
                                Message msg = createMessage(message3);
                                //logging purpose
                                // System.out.println(msg.message+" has arrived fom process 4.");


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
                            } catch (IOException e) {
                                System.out.println("IO exception occured in receiving messages.");
                                break;
                            }
                        }
                    }
                });

                // send 100 messages to compTwo, compThree and compFour
                Thread t4 = new Thread(new Runnable() {
                    public void run() {
                        try {
                            DataOutputStream two_out = new DataOutputStream(clientSocket2.getOutputStream());
                            DataOutputStream three_out = new DataOutputStream(clientSocket3.getOutputStream());
                            DataOutputStream four_out = new DataOutputStream(clientSocket4.getOutputStream());
                            for(int i = 1; i <= 100; i++) {
                                // increment the vector clock of the process by 1 for first bit every time a message is sent
                                vectorClock_lock.lock();
                                vectorClock.set(0, vectorClock.get(0) + 1);
                                // convert the vector clock to string and send it as part of the message
                                String vectorClockString = vectorClock.toString();
                                // format the messages as "1,Message:1,100", "1,Message:2,200" as "from, message, timestamp".
                                String msgToSent = processName+","+"Message:"+i+","+vectorClockString;
                                //Wait for a random amount of time in the range(0,10] milliseconds.
                                try {
                                    Thread.sleep((long)(Math.random() * 10));
                                } catch (InterruptedException e) {
                                    System.out.println("Thread interrupted.");
                                }
                                two_out.writeUTF(msgToSent);
                                // two_out.writeUTF("CompOne: Message " + i + ".");
                                two_out.flush();
                                three_out.writeUTF(msgToSent);
                                three_out.flush();
                                four_out.writeUTF(msgToSent);
                                four_out.flush();
                                vectorClock_lock.unlock();
                            }
                            two_out.writeUTF("exit");
                            two_out.flush();
                            three_out.writeUTF("exit");
                            three_out.flush();
                            four_out.writeUTF("exit");
                            four_out.flush();
                        } catch (IOException e) {
                            System.out.println("IO exception occured in sending messages.");
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
                    System.out.println("Thread interrupted.");
                }
                // clean up the resources
                clientSocket2.close();
                clientSocket3.close();
                clientSocket4.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        CompOne server = new CompOne();
        server.start();
    }
}