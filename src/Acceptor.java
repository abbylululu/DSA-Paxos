import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

public class Acceptor extends Thread{
    private Integer maxPrepare;
    private Integer accNum;
    private String accVal;
    private DatagramSocket receiveSocket;
    private DatagramSocket sendSocket;
    private boolean running;
    private byte[] buffer = new byte[65535];
    ArrayList<HashMap<String, String>> sitesInfo;
    protected BlockingQueue queue = null;

    public Acceptor(BlockingQueue queue, DatagramSocket receiveSocket, DatagramSocket sendSocket,
                    ArrayList<HashMap<String, String>> sitesInfo, ReservationSys mySite) {
        this.maxPrepare = 0;
        this.accNum = -1;
        this.accVal = null;
        this.receiveSocket = receiveSocket;
        this.sendSocket = sendSocket;
        this.running = true;
        this.sitesInfo = sitesInfo;
        this.queue = queue;
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
                        !this.sitesInfo.get(i).get("siteId").equals(this.mySite.getSiteId())) {
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
            String[] getCommand = recvMessage.split(Pattern.quote("("));
            if (getCommand[0].equals("promise") || getCommand[0].equals("ack")
                    || getCommand[0].equals("nack")) {// A->P
                //TODO: Using blocking queue transporting to main thread

            } else if (getCommand[0].equals("prepare") || getCommand[0].equals("accept")) {// P->A
                if (getCommand[0].equals("prepare")) {
                    recvPrepare(Integer.parseInt(getCommand[1].split(Pattern.quote(")"))[0]), senderIp);
                } else {
                    String NV = getCommand[1].split(Pattern.quote(")"))[0];
                    Integer N = Integer.parseInt(NV.split(",")[0]);
                    String V = NV.split(",")[1];
                    recvAccept(N, V, senderIp);
                }

            } else if (getCommand[0].equals("accepted")) {// A->DL
                //TODO: Using blocking queue transporting to main thread

            } else if (getCommand[0].equals("commit")) {// DL->L
                //TODO: Using blocking queue transporting to main thread

            } else {
                System.out.println("[test]Oops, wrong msg!");
            }

            this.buffer = new byte[65535];//reset
        }
        receiveSocket.close();
    }

    // Deserialize the byte array and reconstruct the object
    public static Object deserialize(byte[] buffer) throws IOException, ClassNotFoundException{
        ByteArrayInputStream byteStream = new ByteArrayInputStream(buffer);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);
        return objStream.readObject();
    }

    // @From: Proposer
    // @To: Acceptor(current)
    public void recvPrepare(Integer n, String senderIP) {
        if (n > this.maxPrepare) {
            this.maxPrepare = n;
            sendPromise(senderIP);
        } else {
            sendNack(senderIP);
        }
    }

    // @From: Proposer
    // @To: Acceptor(current)
    public void recvAccept(Integer n, String v, String senderIP) {
        if (n >= this.maxPrepare) {
            this.accNum = n;
            this.accVal = v;
            this.maxPrepare = n;
            sendAck(senderIP);
            sendAccepted(senderIP);
        } else {
            sendNack(senderIP);
        }
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendPromise(String senderIP) {
        String promiseMsg = "promise(" + Integer.toString(this.accNum) + "," + this.accVal + ")";
        // TODO: Call UDP send function
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendNack(String senderIP) {
        String ackMsg = "nack(" + Integer.toString(this.maxPrepare) + ")";
        // TODO: Call UDP send function
    }

    // @From: Acceptor(current)
    // @To: Proposer
    public void sendAck(String senderIP) {
        // TODO: Call UDP send function
    }

    // @From: Acceptor(current)
    // @To: Distinguished Learner
    // The proposer that proposed this proposal is the DL
    public void sendAccepted(String senderIP) {
        String acceptedMsg = "accepted(" + Integer.toString(this.accNum) + "," + this.accVal + ")";
        // TODO: Call UDP send function
    }

}
