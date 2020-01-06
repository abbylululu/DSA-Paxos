package Roles;

import Utils.SendUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Proposer {
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
    private DatagramSocket sendSocket; // send
    private BlockingQueue<String> proposerQueue; // receive

    private int currentProposalNumber;
    private int currentLogSlot; // from user input
    private String currentProposalVal; // from user input

    private TreeMap<String, Map.Entry<Integer, String>> promiseQueue;
    private int ackCounter;

    @Contract(pure = true)
    public Proposer(int uid, ArrayList<HashMap<String, String>> sitesInfo,
                    DatagramSocket sendSocket, BlockingQueue<String> blocking_queue) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
        this.sendSocket = sendSocket;
        this.currentProposalNumber = uid;
        this.proposerQueue = blocking_queue;
        this.promiseQueue = new TreeMap<>();
    }

    private boolean synodPhase1() {
        int majority = this.majority();
        // 1. send prepare for proposing
        this.sendPrepare();

        // 2. blocking on receiving promise
        int maxAccNum = -1;
        String maxVal = null;
        // time out after 10000 millis
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 1000) {
            String curMsg = this.proposerQueue.poll();
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
                        }
                    }
                    if (maxVal != null) {
                        this.currentProposalVal = maxVal;
                    }
                    reset();
                    return true;
                }
            } else if (splitted[0].equals("nack")) {
                recvNack(curMsg);
            }
        }
        reset();
        return false;
    }

    private void reset() {
        this.promiseQueue.clear();
        this.ackCounter = 0;
    }


    private boolean synodPhase2() throws IOException {
        int majority = this.majority();

        sendAccept(this.currentProposalNumber, this.currentProposalVal);
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 1000) {
            String curMsg = this.proposerQueue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("ack")) {
                this.ackCounter++;

                if (this.ackCounter >= majority) {
                    reset();
                    return true;
                }
            } else if (splitted[0].equals("nack")) {
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
        int cnt = 3;
        while (cnt > 0) {
            if (!synodPhase1()) {
                cnt--;
                continue;
            }
            if (!synodPhase2()) {
                cnt--;
                continue;
            }
            break;
        }

        if (cnt <= 0) {
            return false;
        }

        // commit
        sendCommit(this.currentProposalNumber, this.currentProposalVal);
        return this.currentProposalVal.equals(val);
    }


    public boolean startOptimizedSynod(Integer logSlot, String val) throws IOException {
        this.currentLogSlot = logSlot;
        this.currentProposalVal = val;
        this.currentProposalNumber = 0;
        reset();
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
            if (!synodPhase2()) {
                cnt--;
                continue;
            }
            break;
        }

        if (cnt <= 0) {
            return false;
        }

        // commit
        sendCommit(this.currentProposalNumber, this.currentProposalVal);
        return this.currentProposalVal.equals(val);
    }


    private int majority() {
        int numSites = this.sitesInfo.size();
        return (int) Math.ceil(numSites / 2.0);
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

    private void sendPrepare() {
        // increment the proposal number
        this.currentProposalNumber += this.sitesInfo.size();

        // generate message for sending: "prepare curPropNum log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(this.currentProposalNumber);
        sb.append(" ");
        sb.append(this.currentLogSlot);

        // send prepare to all sites
        for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
            String recvIp = stringStringHashMap.get("ip");
            SendUtils prepare = new SendUtils(recvIp, Integer.parseInt(stringStringHashMap.get("startPort")), this.sendSocket, sb.toString());
            prepare.start();
        }
        System.err.printf("Roles.Proposer<%s> sends prepare(%d)to all sites%n",
                this.sitesInfo.get(uid).get("siteId"), this.currentProposalNumber);
    }


    private void recvPromise(@NotNull String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int accNum = 0;
        StringBuilder accVal = null;
        if (!splitted[1].equals("null")) {
            accNum = Integer.parseInt(splitted[1]);
            accVal = new StringBuilder();
            for (int i = 2; i < splitted.length - 2; i++) {
                accVal.append(splitted[i]).append(" ");
            }
            accVal = new StringBuilder(accVal.toString().trim());
        }
        String sender_ip = splitted[splitted.length - 1];

        // store in my promise queue for current log slot
        // promise queues: slot_queue(siteIp -> pair(accNum, accVal)
        assert accVal != null;
        Map.Entry<Integer, String> recvAccs = new AbstractMap.SimpleEntry<>(accNum, accVal.toString());
        this.promiseQueue.put(sender_ip, recvAccs);
    }


    private void recvNack(@NotNull String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int recvMaxNum = Integer.parseInt(splitted[1]);
        this.currentProposalNumber = Math.max(recvMaxNum, this.currentProposalNumber);
    }

    private void sendAccept(int proposalNumber, String proposalVal) throws IOException {
        String msg = String.format("accept %d %s %d", proposalNumber,
                proposalVal, this.currentLogSlot);

        for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
            String recvIp = stringStringHashMap.get("ip");
            SendUtils accept = new SendUtils(recvIp, Integer.parseInt(stringStringHashMap.get("startPort")), this.sendSocket, msg);
            accept.start();
        }
        System.err.printf("Roles.Proposer<%s> sends accept(%d,'%s') to all sites%n",
                this.sitesInfo.get(uid).get("siteId"), proposalNumber, proposalVal);
    }


    private void sendCommit(int accNum, String accVal) throws IOException {
        String msg = String.format("commit %d %s %d %s", accNum, accVal,
                this.currentLogSlot, this.sitesInfo.get(uid).get("ip"));

        for (HashMap<String, String> stringStringHashMap : this.sitesInfo) {
            String recvIp = stringStringHashMap.get("ip");
            SendUtils commit;
            commit = new SendUtils(recvIp, Integer.parseInt(stringStringHashMap.get("startPort")), this.sendSocket, msg);
            commit.start();
        }
        System.err.printf("Distinguished Roles.Learner<%s> sends commit ('%s')to all sites%n",
                this.sitesInfo.get(uid).get("siteId"), accVal);
    }
}