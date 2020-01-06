package Roles;

import Messages.Record;
import Utils.SendUtils;
import App.Host;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

public class Acceptor extends Thread {
    //log_slot to maxPrepare
    private TreeMap<Integer, Integer> maxPrepare;
    private TreeMap<Integer, Integer> accNum;
    private TreeMap<Integer, String> accVal;
    static TreeMap<Integer, String> proposerIp;
    private DatagramSocket receiveSocket;
    private DatagramSocket sendSocket;
    private boolean running;
    private byte[] buffer = new byte[65535];
    private ArrayList<HashMap<String, String>> sitesInfo;
    private BlockingQueue<String> proposerQueue;
    private BlockingQueue<String> learnerQueue;


    public Acceptor(BlockingQueue<String> proposerQueue, BlockingQueue<String> learnerQueue,
                    DatagramSocket receiveSocket, DatagramSocket sendSocket,
                    ArrayList<HashMap<String, String>> sitesInfo) throws IOException, ClassNotFoundException {
        this.maxPrepare = new TreeMap<>();
        this.accNum = new TreeMap<>();
        this.accVal = new TreeMap<>();
        proposerIp = new TreeMap<>();
        File acceptorFile = new File(Host.curSiteId + "acceptor.txt");
        if (acceptorFile.exists()) {
            recoverAcceptor();
        }
        this.receiveSocket = receiveSocket;
        this.sendSocket = sendSocket;
        this.running = true;
        this.sitesInfo = sitesInfo;
        this.proposerQueue = proposerQueue;
        this.learnerQueue = learnerQueue;
    }

    public void run() {
        DatagramPacket packet;

        while (this.running) {
            // Receive from other server
            packet = new DatagramPacket(this.buffer, this.buffer.length);
            try {
                // blocks until a msg arrives
                receiveSocket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Processing information
            String senderIp = packet.getAddress().getHostAddress();
            String senderId = null;
            for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
                if (stringStringHashMap.get("ip").equals(senderIp) &&
                        !stringStringHashMap.get("siteId").equals(Host.curSiteId)) {
                    senderId = stringStringHashMap.get("siteId");
                    break;
                }
            }

            // handle the receiving messages
            String recvMessage = null;
            try {
                recvMessage = (String) deserialize(packet.getData());

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            // parse the string
            assert recvMessage != null;
            String[] getCommand = recvMessage.split(" ");//prepare
            if (getCommand[0].equals("lastSeen")) {
                // update last seen
                for (int i = 1; i < getCommand.length; i++) {
                    String[] oneTwo = getCommand[i].split(",");
                    Host.lastSeen.put(oneTwo[0], oneTwo[1]);
                }
                // keep in stable storage
                try {
                    Host.storeLastSeen();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            switch (getCommand[0]) {
                case "promise":
                case "ack":
                case "nack": // A->P
                    System.err.printf("Roles.Proposer<%s> received %s from %s%n",
                            Host.curSiteId,
                            recvMessage,
                            ipToID(senderIp));

                    try {
                        this.proposerQueue.put(recvMessage);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    break;
                case "prepare":
                case "accept": // P->A
                    if (getCommand[0].equals("prepare")) {
                        try {
                            System.err.printf("Roles.Acceptor<%s> received prepare(%s) from %s%n",
                                    Host.curSiteId,
                                    getCommand[1],
                                    ipToID(senderIp));
                            recvPrepare(Integer.parseInt(getCommand[1]), senderIp, Integer.parseInt(getCommand[2]));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            System.err.printf("Roles.Acceptor<%s> received %s from %s%n",
                                    Host.curSiteId,
                                    recvMessage,
                                    ipToID(senderIp));
                            if (getCommand[2].equals("cancel")) {
                                recvAccept(Integer.parseInt(getCommand[1]), getCommand[2] + " " + getCommand[3],
                                        senderIp, Integer.parseInt(getCommand[4]));
                            } else {
                                StringBuilder accVal = new StringBuilder();
                                for (int i = 2; i < getCommand.length - 1; i++) {
                                    accVal.append(getCommand[i]);
                                    accVal.append(" ");
                                }
                                recvAccept(Integer.parseInt(getCommand[1]),
                                        accVal.toString().trim(), senderIp,
                                        Integer.parseInt(getCommand[getCommand.length - 1]));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    break;
                case "commit": // A->DL & DL->L
                    System.err.printf("Roles.Learner<%s> received %s from %s%n",
                            Host.curSiteId,
                            recvMessage,
                            ipToID(senderIp));
                    try {
                        this.learnerQueue.put(recvMessage);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    break;
                case "MaximumLog":
                    try {
                        acceptorSend(senderIp, "Max " + Learner.getMaxLogSlot());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "Max":
                    Learner.newMax = Integer.parseInt(
                            getCommand[1]) > Learner.newMax ? Integer.parseInt(getCommand[1]) : Learner.newMax;

                    try {
                        learnerQueue.put(recvMessage);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + getCommand[0]);
            }

            this.buffer = new byte[65535];//reset
        }
        receiveSocket.close();
    }

    // Deserialize the byte array and reconstruct the object
    public static Object deserialize(byte[] buffer) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(buffer);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);
        return objStream.readObject();
    }

    // @From: Roles.Proposer
    // @To: Roles.Acceptor(current)
    private void recvPrepare(Integer n, String senderIP, Integer logSlot) throws IOException {
        if (!this.maxPrepare.containsKey(logSlot) || n > this.maxPrepare.get(logSlot)) {
            this.maxPrepare.put(logSlot, n);
            recordAcceptor();
            sendPromise(senderIP, logSlot);
        } else {
            sendNack(senderIP, logSlot);
        }
        Host.sendLastSeen(sendSocket);
    }

    // @From: Roles.Proposer
    // @To: Roles.Acceptor(current)
    // ToDo: check null pointer
    private void recvAccept(Integer n, String v, String senderIP, Integer logSlot) throws IOException {
        if (!this.maxPrepare.containsKey(logSlot)) this.maxPrepare.put(logSlot, 0);
        if (this.maxPrepare.containsKey(logSlot) && n >= this.maxPrepare.get(logSlot)) {
            this.accNum.put(logSlot, n);
            this.accVal.put(logSlot, v);
            this.maxPrepare.put(logSlot, n);
            recordAcceptor();
            sendAck(senderIP, logSlot);
            sendAccepted(senderIP, logSlot);
        } else {
            sendNack(senderIP, logSlot);
        }
        Host.sendLastSeen(sendSocket);
    }

    // @From: Roles.Acceptor(current)
    // @To: Roles.Proposer
    private void sendPromise(String senderIP, Integer logSlot) throws IOException {
        String promiseMsg;
        if (this.accNum.containsKey(logSlot) && this.accVal.containsKey(logSlot)) {
            promiseMsg = String.format("promise %d %s %d %s",
                    this.accNum.get(logSlot),
                    this.accVal.get(logSlot),
                    logSlot,
                    Host.curIp);
        } else {
            String curAccNum = "null", curAccVal = "null";
            if (this.accNum.containsKey(logSlot)) {
                curAccNum = Integer.toString(this.accNum.get(logSlot));
            }
            if (this.accVal.containsKey(logSlot)) {
                curAccVal = this.accVal.get(logSlot);
            }
            promiseMsg = String.format("promise %s %s %d %s", curAccNum, curAccVal, logSlot, Host.curIp);
        }
        acceptorSend(senderIP, promiseMsg);
        System.err.printf("Roles.Acceptor<%s> sends %s to %s%n",
                Host.curSiteId,
                promiseMsg,
                ipToID(senderIP));
        Host.sendLastSeen(sendSocket);
    }

    // @From: Roles.Acceptor(current)
    // @To: Roles.Proposer
    private void sendNack(String senderIP, Integer logSlot) throws IOException {
        String maxNum = "0";
        if (this.maxPrepare.containsKey(logSlot)) {
            maxNum = Integer.toString(this.maxPrepare.get(logSlot));
        }
        String nackMsg = String.format("nack %s %s", maxNum, Host.curIp);
        acceptorSend(senderIP, nackMsg);
        System.err.printf("Roles.Acceptor<%s> sends nack(%s) to %s%n",
                Host.curSiteId,
                maxNum,
                ipToID(senderIP));
        Host.sendLastSeen(sendSocket);
    }

    // @From: Roles.Acceptor(current)
    // @To: Roles.Proposer
    private void sendAck(String senderIP, Integer logSlot) throws IOException {
        if (!proposerIp.containsKey(logSlot)) {
            proposerIp.put(logSlot, senderIP);
        }
        String ackMsg = String.format("ack %d %s", this.maxPrepare.get(logSlot), proposerIp.get(logSlot));
        acceptorSend(senderIP, ackMsg);
        System.err.printf("Roles.Acceptor<%s> sends ack(%d) to %s%n",
                Host.curSiteId,
                this.maxPrepare.get(logSlot),
                ipToID(senderIP));
        Host.sendLastSeen(sendSocket);
    }

    // @From: Roles.Acceptor(current)
    // @To: Distinguished Roles.Learner
    // The proposer that proposed this proposal is the DL
    private void sendAccepted(String senderIP, Integer logSlot) throws IOException {
        String acceptedMsg = String.format("accepted %d %s %d %s",
                this.accNum.get(logSlot),
                this.accVal.get(logSlot),
                logSlot,
                Host.curIp);
        acceptorSend(senderIP, acceptedMsg);
        System.err.printf("Roles.Acceptor<%s> sends accepted(%d,%s) to %s%n",
                Host.curSiteId,
                this.accNum.get(logSlot),
                this.accVal.get(logSlot),
                ipToID(senderIP));
        Host.sendLastSeen(sendSocket);
    }

    private void acceptorSend(String senderIP, String message) throws IOException {
        InetAddress targetIP = InetAddress.getByName(senderIP);
        byte[] sendArray = SendUtils.serialize(message);
        String receivePort = null;
        for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
            if (stringStringHashMap.get("ip").equals(senderIP)) {
                receivePort = stringStringHashMap.get("startPort");
                break;
            }
        }
        assert receivePort != null;
        DatagramPacket sendPacket = new DatagramPacket(sendArray, sendArray.length, targetIP, Integer.parseInt(receivePort));
        this.sendSocket.send(sendPacket);
        Host.sendLastSeen(sendSocket);
    }

    private void recordAcceptor() throws IOException {
        Record log = new Record(this.maxPrepare, this.accNum, this.accVal, proposerIp);
        byte[] output = SendUtils.serialize(log);
        File file = new File(Host.curSiteId + "acceptor.txt");
        FileOutputStream fos;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    private void recoverAcceptor() throws IOException, ClassNotFoundException {
        Record recover = (Record) deserialize(readFromFile(Host.curSiteId + "acceptor.txt"));
        this.maxPrepare = recover.getMaxPrepare();
        this.accNum = recover.getAccNum();
        this.accVal = recover.getAccVal();
        proposerIp = recover.getProposerIp();
    }

    @NotNull
    public static byte[] readFromFile(String fileName) throws IOException {
        File file = new File(fileName);
        byte[] getBytes;
        getBytes = new byte[(int) file.length()];
        InputStream is = new FileInputStream(file);
        is.read(getBytes);
        is.close();
        return getBytes;
    }

    @Nullable
    private String ipToID(String ip) {
        for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
            if (stringStringHashMap.get("ip").equals(ip)) {
                return stringStringHashMap.get("siteId");
            }
        }
        return null;
    }
}



