import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class Acceptor extends Thread {
    private HashMap<Integer, Integer> maxPrepare;//log_slot to maxPrepare
    private HashMap<Integer, Integer> accNum;
    private HashMap<Integer, String> accVal;
    private DatagramSocket receiveSocket;
    private DatagramSocket sendSocket;
    private boolean running;
    private byte[] buffer = new byte[65535];
    private ArrayList<HashMap<String, String>> sitesInfo;
    private String siteId;
    private BlockingQueue queue = null;


    public Acceptor(BlockingQueue queue, DatagramSocket receiveSocket, DatagramSocket sendSocket,
                    ArrayList<HashMap<String, String>> sitesInfo, String siteId) throws IOException, ClassNotFoundException {
        File acceptorFile = new File("acceptor.txt");
        if (acceptorFile.exists()) {
            recoverAcceptor();
        } else {
            this.maxPrepare = new HashMap<>();
            this.accNum = new HashMap<>();
            this.accVal = new HashMap<>();
        }
        this.receiveSocket = receiveSocket;
        this.sendSocket = sendSocket;
        this.running = true;
        this.sitesInfo = sitesInfo;
        this.queue = queue;
        this.siteId = siteId;
    }

    public void run() {
        DatagramPacket packet = null;

        while (this.running) {
            // Receive from other server
            packet = new DatagramPacket(this.buffer, this.buffer.length);
            try {
                receiveSocket.receive(packet);// blocks until a msg arrives
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Processing information
            String senderIp = packet.getAddress().getHostAddress();
            String senderId = null;
            for (int i = 0; i < this.sitesInfo.size(); i++) {
                if (this.sitesInfo.get(i).get("ip").equals(senderIp) &&
                        !this.sitesInfo.get(i).get("siteId").equals(this.siteId)) {
                    senderId = this.sitesInfo.get(i).get("siteId");
//                    System.out.println("[test] Got something from site " + senderId);
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
            System.out.println("[test] " + siteId + " receives " + recvMessage);
            String[] getCommand = recvMessage.split(" ");//prepare
            if (getCommand[0].equals("promise") || getCommand[0].equals("ack")
                    || getCommand[0].equals("nack")) {// A->P
                String transmission = null;
                if (getCommand[0].equals("promise")) {
                    transmission = "promise " + getCommand[1] + " " + getCommand[2] + " " + senderIp;

                } else if (getCommand[0].equals("ack")) {
                    transmission = "ack " + getCommand[1] + " " + senderIp;

                } else {
                    transmission = "nack " + getCommand[1] + " " + senderIp;
                }
                System.err.println("Proposer<" + siteId + "> received " + transmission + " from " + ipToID(senderIp));
                System.out.println("[test]A->P transmission through block queue is: " + transmission);

                try {
                    this.queue.put(transmission);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (getCommand[0].equals("prepare") || getCommand[0].equals("accept")) {// P->A
                if (getCommand[0].equals("prepare")) {
                    try {
                        System.err.println("Acceptor<" + siteId + "> received prepare(" + getCommand[1] + ") from " + ipToID(senderIp));
                        recvPrepare(Integer.parseInt(getCommand[1]), senderIp, Integer.parseInt(getCommand[2]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        System.err.println("Acceptor<" + siteId + "> received accept(" +
                                getCommand[2] + "," + getCommand[3] + ") from " + ipToID(senderIp));
                        recvAccept(Integer.parseInt(getCommand[1]), getCommand[2] + " " + getCommand[3]
                                + " " + getCommand[4], senderIp, Integer.parseInt(getCommand[5]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } else if (getCommand[0].equals("accepted") || getCommand[0].equals("commit")) {// A->DL & DL->L
                String transmission = null;
                if (getCommand[0].equals("accepted")) {
                    transmission = recvMessage + " " + senderIp;
                } else {
                    transmission = recvMessage + " " + senderIp;
                }
                System.err.println("Learner<" + siteId + "> received " + transmission + " from " + ipToID(senderIp));
                System.out.println("[test] A To L transmission through block queue is: " + transmission);

                try {
                    this.queue.put(transmission);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                System.out.println("[test]Oops, wrong msg!");
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

    // Serialize the CommunicateInfo to byte array
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        return bos.toByteArray();
    }

    // @From: Proposer
    // @To: Acceptor(current)
    public void recvPrepare(Integer n, String senderIP, Integer logSlot) throws IOException {
        if (n > this.maxPrepare.get(logSlot)) {
            this.maxPrepare.put(logSlot, n);
            recordAcceptor();
            sendPromise(senderIP, logSlot);
        } else {
            sendNack(senderIP, logSlot);
        }
    }

    // @From: Proposer
    // @To: Acceptor(current)
    public void recvAccept(Integer n, String v, String senderIP, Integer logSlot) throws IOException {
        if (n >= this.maxPrepare.get(logSlot)) {
            this.accNum.put(logSlot, n);
            this.accVal.put(logSlot, v);
            this.maxPrepare.put(logSlot, n);
            recordAcceptor();
            sendAck(senderIP, logSlot);
            sendAccepted(senderIP, logSlot);
        } else {
            sendNack(senderIP, logSlot);
        }
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendPromise(String senderIP, Integer logSlot) throws IOException {
        String promiseMsg = "promise " + Integer.toString(this.accNum.get(logSlot)) + " "
                + this.accVal.get(logSlot) + " " + Integer.toString(logSlot) + " " + senderIP;
        acceptorSend(senderIP, promiseMsg);
        System.err.println("Acceptor<" + siteId + "> sends promise(" + Integer.toString(this.accNum.get(logSlot)) + ","
                + this.accVal.get(logSlot) +") to " + ipToID(senderIP));
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendNack(String senderIP, Integer logSlot) throws IOException {
        String nackMsg = "nack " + Integer.toString(this.maxPrepare.get(logSlot)) + " " + senderIP;
        acceptorSend(senderIP, nackMsg);
        System.err.println("Acceptor<" + siteId + "> sends nack(" +
                Integer.toString(this.maxPrepare.get(logSlot))  +") to " + ipToID(senderIP));
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendAck(String senderIP, Integer logSlot) throws IOException {
        String ackMsg = "ack " + Integer.toString(this.maxPrepare.get(logSlot)) + " " + senderIP;
        acceptorSend(senderIP, ackMsg);
        System.err.println("Acceptor<" + siteId + "> sends ack(" +
                Integer.toString(this.maxPrepare.get(logSlot))  +") to " + ipToID(senderIP));
    }

    // @From: Acceptor(current)
    // @To: Distinguished Learner
    // The proposer that proposed this proposal is the DL
    public void sendAccepted(String senderIP, Integer logSlot) throws IOException {
        String acceptedMsg = "accepted " + Integer.toString(this.accNum.get(logSlot)) + " "
                + this.accVal.get(logSlot) + " " + Integer.toString(logSlot) + " " + senderIP;
        acceptorSend(senderIP, acceptedMsg);
        System.err.println("Acceptor<" + siteId + "> sends accepted(" +
                Integer.toString(this.accNum.get(logSlot)) + ","
                + this.accVal.get(logSlot)  +") to " + ipToID(senderIP));
    }

    public void acceptorSend(String senderIP, String message) throws IOException {
        InetAddress targetIP = InetAddress.getByName(senderIP);
        byte[] sendArray = serialize(message);
        String receivePort = null;
        for (int j = 0; j < this.sitesInfo.size(); j++) {
            if (this.sitesInfo.get(j).get("ip").equals(senderIP)) {
                receivePort = this.sitesInfo.get(j).get("startPort");
                break;
            }
        }
        assert receivePort != null;
        DatagramPacket sendPacket = new DatagramPacket(sendArray, sendArray.length, targetIP, Integer.parseInt(receivePort));
        this.sendSocket.send(sendPacket);
    }

    private void recordAcceptor() throws IOException {
        Record log = new Record(this.maxPrepare, this.accNum, this.accVal);
        byte[] output = serialize(log);
        File file = new File("acceptor.txt");
        FileOutputStream fos = null;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    private void recoverAcceptor() throws IOException, ClassNotFoundException {
        Record recover = (Record)deserialize(readFromFile());
        this.maxPrepare = recover.getMaxPrepare();
        this.accNum = recover.getAccNum();
        this.accVal = recover.getAccVal();
    }

    public byte[] readFromFile() throws IOException {
        File file = new File("acceptor.txt");
        byte[] getBytes = {};
        getBytes = new byte[(int) file.length()];
        InputStream is = new FileInputStream(file);
        is.read(getBytes);
        is.close();
        return getBytes;
    }

    public String ipToID(String ip) {
        for (int j = 0; j < this.sitesInfo.size(); j++) {
            if (this.sitesInfo.get(j).get("ip").equals(ip)) {
                return this.sitesInfo.get(j).get("siteId");
            }
        }
        return null;
    }
}



