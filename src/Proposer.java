import javafx.util.Pair;

import java.lang.reflect.Array;
import java.net.DatagramSocket;
import java.util.*;

public class Proposer {
    private int uid; // node idx for unique identifier
    private ArrayList<HashMap<String, String>> sitesInfo;
    int sendPort;
    DatagramSocket sendSocket;
    private int current_proposal_number;
    private String reservation; // from user command insert
    private int next_log_slot; // from user command insert

    // blocking queue: for receiving promise and ack
    private Queue<String> blocking_queue;
    // slot -> pair(curPropNum, Reservation(in string form))
    private HashMap<Integer, Pair<Integer, String>> my_proposals;
    // siteIp -> pair(accNum, accVal)
    private HashMap<String, Pair<Integer, String>> promise_queues;
    // slot -> slot_queue(siteIp -> reservation(string form))
    private HashMap<String, String> ack_queues;
//    private ArrayList<Integer> committed_slots;
    // slot -> accVal
    private HashMap<Integer, String> learnt_slots;

    public Proposer(int uid, ArrayList<HashMap<String, String>> sitesInfo, DatagramSocket sendSocket) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
        this.sendPort = Integer.parseInt(sitesInfo.get(uid).get("startPort"));
        this.sendSocket = sendSocket;
        this.current_proposal_number = uid;
        this.next_log_slot = 0;

        this.blocking_queue = new LinkedList<>();
        this.my_proposals = new HashMap<>();
        this.promise_queues = new HashMap<>();
        this.ack_queues = new HashMap<>();
//        this.committed_slots = new ArrayList<>();
        this.learnt_slots = new HashMap<>();
    }

//    public Proposer(int next_log_slot, String reservation, int uid, ArrayList<HashMap<String, String>> sitesInfo, DatagramSocket sendSocket) {
//        this.uid = uid;
//        this.sitesInfo = sitesInfo;
//        this.sendPort = Integer.parseInt(sitesInfo.get(uid).get("startPort"));
//        this.sendSocket = sendSocket;
//        this.current_proposal_number = uid;
//        this.reservation = reservation;
//        this.next_log_slot = next_log_slot;
//
//        this.blocking_queue = new LinkedList<>();
//        this.my_proposals = new HashMap<>();
//        this.promise_queues = new HashMap<>();
//        this.ack_queues = new HashMap<>();
//        this.committed_slots = new ArrayList<>();
//    }

    public void sendPrepare() {
        // choose a proposal number to propose
        this.current_proposal_number += 10;
        int n = this.current_proposal_number;

        // keep a record of my proposals: slot -> proposalNum, reservation(string form)
        Pair<Integer, String> insertInfo = new Pair<>(n, this.reservation);
        this.my_proposals.put(this.next_log_slot, insertInfo);

        // generate message for sending: "prepare curPropNum next_log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(n);
        sb.append(" ");
        sb.append(this.next_log_slot);

        // send prepare to all sites
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send prepare = new Send(recvIp, this.sendPort, this.sendSocket, sb.toString());
            prepare.start();
        }
    }

    // promise form "Promise accNum accVal log_slot sender_ip"
    public void recvPromise(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int accNum = Integer.parseInt(splitted[1]);
        String accVal = splitted[2];
        int log_slot = Integer.parseInt(splitted[3]);
        String sender_ip = splitted[4];

        // store in my promise queue for current log slot
        // promise queues: slot_queue(siteIp -> pair(accNum, accVal)
        Pair<Integer, String> recvAccs = new Pair<>(accNum, accVal);
        this.promise_queues.put(sender_ip, recvAccs);
    }

    // message form: accept proposalNum reservation(string form)
    public void sendAccept(int proposalNum, String reservation, int log_slot) {
        // build the accept message to send
        StringBuilder sb = new StringBuilder();
        sb.append("accept ");
        sb.append(proposalNum);
        sb.append(" ");
        sb.append(reservation);

        // send accept to the same set of majority
        for (Map.Entry<String, Pair<Integer, String>> mapElement: this.promise_queues.entrySet()) {
            String recvIp = mapElement.getKey();
            Send accept = new Send(recvIp, this.sendPort, this.sendSocket, sb.toString());
            accept.start();
        }
    }

    // message form: ack maxPrepNum senderIp
    public void recvAck(String message) {
        // keep a record of ack message
        // ack queue: slot -> slot_queue(siteIp -> reservation(string form))
        // ack would always be dealing with self proposal
        String curSiteIp = this.sitesInfo.get(this.uid).get("ip");
        this.ack_queues.put(curSiteIp, this.reservation);
    }

    // message form: commit accVal logSlot senderIP
    public void sendCommit(String accVal) {
        // build the commit message
        StringBuilder sb = new StringBuilder();
        sb.append("commit ");
        sb.append(accVal);
        sb.append(" ");
        sb.append(this.next_log_slot);
        sb.append(" ");
        sb.append(this.sitesInfo.get(uid).get("ip"));

        // send commit to all sites except self
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            if (i == this.uid) continue;
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send commit = new Send(recvIp, this.sendPort, this.sendSocket, sb.toString());
            commit.start();
        }
    }

    // message form: commit accVal logSlot senderIP
    public void recvCommit(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int logSlot = Integer.parseInt(splitted[2]);
        String accVal = splitted[1];
        this.learnt_slots.put(logSlot, accVal);
    }

    public HashMap<Integer, Reservation> learn(HashMap<Integer, Reservation> curLog) {
        for (Map.Entry<Integer, String> mapElement: this.learnt_slots.entrySet()) {
            int curSlot = mapElement.getKey();
            String curResv = mapElement.getValue();
            curLog.put(curSlot, new Reservation(curResv));
        }
        return curLog;
    }

    // after user input: reserve or cancel, start to propose a log slot
    // return: 1: successful propose 0: unsuccessful propose
    public int propose() {
        // 1. send prepare for proposing
        this.sendPrepare();

        // define majority sites count
        int numSites = this.sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0); // FIXME: Am I right?

        // 2. blocking on receiving promise
        int success = 0;
        int maxAccNum = 0;
        String maxVal = null;
        // time out after 10000 millis
        long startTime = System.currentTimeMillis();
        while(success == 0 && (System.currentTimeMillis() - startTime) < 10000) {
            String curMsg = this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("promise")) {
                // keep a record of every received promise in promise queue
                // format: siteIp -> pair(accNum, accVal)
                recvPromise(curMsg);
                int numPromise = this.promise_queues.size();
                if (numPromise >= majority) {
                    // indicating successful proposal
                    success = 1;
                    // try to choose largest accNum and accVal to send
                    for (Map.Entry<String, Pair<Integer, String>> mapElement : this.promise_queues.entrySet()) {
                        Pair<Integer, String> accEntry = mapElement.getValue();
                        int curAccNum = accEntry.getKey();
                        String curAccString = accEntry.getValue();
                        if (curAccNum > maxAccNum) {
                            maxVal = curAccString;
                        }
                    }
                    // choose my own proposal value to send
                    if (maxVal == null) {
                        maxVal = this.reservation;
                    }
                    // FIXME: what if my own reservation is dumped?
                }
            }
            // when receiving commit for other slots
            else if (splitted[0].equals("commit")) {
                recvCommit(curMsg);
            }
            // if timeout, return 0 to main and retry
            if (success == 0) return 0;
        }

        // 3. send accept, if receiving promise from majority
        if (success == 1) {
            sendAccept(maxAccNum, maxVal, this.next_log_slot);
        }

        // 4. blocking on receiving ack
        startTime = System.currentTimeMillis();
        while(success == 0 && (System.currentTimeMillis() - startTime) < 10000) {
            String curMsg = this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("ack")) {
                recvAck(curMsg);
                int numAck = this.ack_queues.size();
                if (numAck >= majority) {
                    success = 1;
                }
            }
            // when receiving commit for other slots
            else if (splitted[0].equals("commit")) {
                recvCommit(curMsg);
            }
        }
        // if timeout, return 0 to main and retry
        if (success == 0) return 0;
        // successfully proposed, learn the chosen slot
        this.learnt_slots.put(this.next_log_slot, maxVal);

        // 5. change role to learner, send commit
        sendCommit(maxVal);

        return 1;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public ArrayList<HashMap<String, String>> getSitesInfo() {
        return sitesInfo;
    }

    public void setSitesInfo(ArrayList<HashMap<String, String>> sitesInfo) {
        this.sitesInfo = sitesInfo;
    }

    public int getSendPort() {
        return sendPort;
    }

    public void setSendPort(int sendPort) {
        this.sendPort = sendPort;
    }

    public DatagramSocket getSendSocket() {
        return sendSocket;
    }

    public void setSendSocket(DatagramSocket sendSocket) {
        this.sendSocket = sendSocket;
    }

    public int getCurrent_proposal_number() {
        return current_proposal_number;
    }

    public void setCurrent_proposal_number(int current_proposal_number) {
        this.current_proposal_number = current_proposal_number;
    }

    public String getReservation() {
        return reservation;
    }

    public void setReservation(String reservation) {
        this.reservation = reservation;
    }

    public int getNext_log_slot() {
        return next_log_slot;
    }

    public void setNext_log_slot(int next_log_slot) {
        this.next_log_slot = next_log_slot;
    }

    public Queue<String> getBlocking_queue() {
        return blocking_queue;
    }

    public void setBlocking_queue(Queue<String> blocking_queue) {
        this.blocking_queue = blocking_queue;
    }

    public HashMap<Integer, Pair<Integer, String>> getMy_proposals() {
        return my_proposals;
    }

    public void setMy_proposals(HashMap<Integer, Pair<Integer, String>> my_proposals) {
        this.my_proposals = my_proposals;
    }

    public HashMap<String, Pair<Integer, String>> getPromise_queues() {
        return promise_queues;
    }

    public void setPromise_queues(HashMap<String, Pair<Integer, String>> promise_queues) {
        this.promise_queues = promise_queues;
    }

    public HashMap<String, String> getAck_queues() {
        return ack_queues;
    }

    public void setAck_queues(HashMap<String, String> ack_queues) {
        this.ack_queues = ack_queues;
    }

//    public ArrayList<Integer> getCommitted_slots() {
//        return committed_slots;
//    }
//
//    public void setCommitted_slots(ArrayList<Integer> committed_slots) {
//        this.committed_slots = committed_slots;
//    }
}
