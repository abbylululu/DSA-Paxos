import java.io.IOException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Proposer {
    // ------------------MEMBER VARS------------------ //
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
    private DatagramSocket sendSocket; // send
    private BlockingQueue<String> proposerQueue; // receive

    private int currentProposalNumber;
    private int currentLogSlot; // from user input
    private String currentProposalVal; // from user input

    private TreeMap<String, Map.Entry<Integer, String>> promiseQueue;
    private int ackCounter;

    // ------------------CONSTRUCTOR------------------ //


    public Proposer(int uid, ArrayList<HashMap<String, String>> sitesInfo, DatagramSocket sendSocket, BlockingQueue<String> blocking_queue) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
        this.sendSocket = sendSocket;
        this.currentProposalNumber = uid;
        this.proposerQueue = blocking_queue;
        this.promiseQueue = new TreeMap<>();
    }

    // ------------------HELPER------------------ //
    public boolean synodPhase1() {
        int majority = this.majority();
        // 1. send prepare for proposing
        this.sendPrepare();

        // 2. blocking on receiving promise
        int maxAccNum = -1;
        String maxVal = null;
        // time out after 10000 millis
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 5000) {
            String curMsg = (String) this.proposerQueue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("promise")) {
                recvPromise(curMsg);
                int numPromise = this.promiseQueue.size();
                if (numPromise >= majority) {
                    for (Map.Entry<String, Map.Entry<Integer, String>> mapElement : this.promiseQueue.entrySet()) {
                        Map.Entry<Integer, String> accEntry = mapElement.getValue();
                        int curAccNum = accEntry.getKey();
                        String curAccString = accEntry.getValue();
                        if (curAccNum > maxAccNum && curAccString != null) {
                            maxVal = curAccString;

                            System.out.println("****now maxVal becomes: " + maxVal);
                        }
                    }
                    if (maxVal != null) {
                        this.currentProposalVal = maxVal;
                    }
                    reset();
                    return true;
                }
            }
            else if (splitted[0].equals("nack")) {
                recvNack(curMsg);
            }
        }
        reset();
        return false;
    }

    public void reset() {
        this.promiseQueue.clear();
        this.ackCounter = 0;
    }


    public boolean synodPhase2() throws IOException {
        int majority = this.majority();

        sendAccept(this.currentProposalNumber, this.currentProposalVal);
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 5000) {
            String curMsg = (String) this.proposerQueue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("ack")) {
                this.ackCounter++;

                if (this.ackCounter >= majority) {
                    reset();
                    return true;
                }
            }
            else if (splitted[0].equals("nack")) {
                recvNack(curMsg);
            }
        }
        reset();
        return false;
    }


    public boolean startSynod(Integer logSlot, String val) throws IOException {
        this.currentLogSlot = logSlot;
        this.currentProposalVal = val;
        this.currentProposalNumber = this.uid;
        reset();

        // from user input
        System.out.println("****propose for log slot: " + this.currentLogSlot +
                " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);

        int cnt = 3;
        while (cnt > 0) {
            if (!synodPhase1()) {
                cnt--;
                continue;
            }
            if(!synodPhase2()) {
                cnt--;
                continue;
            }
            break;
        }

        if (cnt <= 0) {
            System.out.println("****failed proposing for log slot: " + this.currentLogSlot +
                    " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);
            return false;
        }

        // commit
        System.out.println("****successfully proposed for log slot: " + this.currentLogSlot +
                " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);
        sendCommit(this.currentProposalNumber, this.currentProposalVal);
        return this.currentProposalVal.equals(val);
    }


    public boolean startOptimizedSynod(Integer logSlot, String val) throws IOException {
        this.currentLogSlot = logSlot;
        this.currentProposalVal = val;
        this.currentProposalNumber = 0;
        reset();

        System.out.println("****propose for log slot: " + this.currentLogSlot +
                " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);

        if (synodPhase2()) {
            // commit
            sendCommit(this.currentProposalNumber, this.currentProposalVal);
            return this.currentProposalVal.equals(val);
        }

        int cnt = 2;
        while (cnt > 0) {
            if (!synodPhase1()) {
                cnt--;
                continue;
            }
            if(!synodPhase2()) {
                cnt--;
                continue;
            }
            break;
        }

        if (cnt <= 0) {
            System.out.println("****failed proposing for log slot: " + this.currentLogSlot +
                    " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);
            return false;
        }

        // commit
        System.out.println("****successfully proposed for log slot: " + this.currentLogSlot +
                " with proposal value & number: " + this.currentProposalVal + " & " + this.currentProposalNumber);
        sendCommit(this.currentProposalNumber, this.currentProposalVal);
        return this.currentProposalVal.equals(val);
    }


    public int majority() {
        int numSites = this.sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0);
        return majority;
    }

    public int getCurrentProposalNumber() {
        return currentProposalNumber;
    }

    public void setCurrentProposalNumber(int currentProposalNumber) {
        this.currentProposalNumber = currentProposalNumber;
    }

    public int getCurrentLogSlot() {
        return currentLogSlot;
    }

    public void setCurrentLogSlot(int currentLogSlot) {
        this.currentLogSlot = currentLogSlot;
    }

    public String getCurrentProposalVal() {
        return currentProposalVal;
    }

    public void setCurrentProposalVal(String currentProposalVal) {
        this.currentProposalVal = currentProposalVal;
    }

    public void sendPrepare() {
        // increment the proposal number
        this.currentProposalNumber += this.sitesInfo.size();

        // generate message for sending: "prepare curPropNum log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(this.currentProposalNumber);
        sb.append(" ");
        sb.append(this.currentLogSlot);

        // send prepare to all sites
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send prepare = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
            prepare.start();
        }

//        System.err.println("sending prepare(" + this.current_proposal_number + ")to all sites");
        System.err.println("Proposer<" + this.sitesInfo.get(uid).get("siteId") + "> sends prepare(" + this.currentProposalNumber + ")to all sites");
    }


    public void recvPromise(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int accNum = 0;
        String accVal = null;
        if (!splitted[1].equals("null")) {
            accNum = Integer.parseInt(splitted[1]);
            accVal = "";
            for (int i = 2; i < splitted.length - 2; i++) {
                accVal = accVal + splitted[i] + " ";
            }
            accVal = accVal.trim();
        }
        String sender_ip = splitted[splitted.length - 1];

        // store in my promise queue for current log slot
        // promise queues: slot_queue(siteIp -> pair(accNum, accVal)
        Map.Entry<Integer, String> recvAccs = new AbstractMap.SimpleEntry<Integer, String>(accNum, accVal);
        this.promiseQueue.put(sender_ip, recvAccs);
    }


    public void recvNack(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int recvMaxNum = Integer.parseInt(splitted[1]);
        this.currentProposalNumber = Math.max(recvMaxNum, this.currentProposalNumber);
    }

    public void sendAccept(int proposalNumber, String proposalVal) throws IOException {
        String msg = String.format("accept %d %s %d", proposalNumber,
                proposalVal, this.currentLogSlot);

//        System.out.println("****accept from apple is: " + msg);

        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send accept = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, msg);
            accept.start();
        }

//        System.err.println("sending accept(" + proposalNumber + "," + "'" + proposalVal + "') to all sites");
        System.err.println("Proposer<" + this.sitesInfo.get(uid).get("siteId") + "> sends accept(" + proposalNumber + "," + "'" + proposalVal + "') to all sites");
        Host.sendLastSeen(sendSocket);
    }


    public void sendCommit(int accNum, String accVal) throws IOException {
        String msg = String.format("commit %d %s %d %s", accNum, accVal,
                this.currentLogSlot, this.sitesInfo.get(uid).get("ip"));

        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send commit = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, msg);
            commit.start();
        }

//        System.err.println("sending commit ('" + accVal + "')to all sites");
        System.err.println("Distinguished Learner<" + this.sitesInfo.get(uid).get("siteId") + "> sends commit ('" + accVal + "')to all sites");
        Host.sendLastSeen(sendSocket);
    }
}