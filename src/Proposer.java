import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Proposer {
    // ------------------MEMBER VARS------------------ //
    private int uid; // node idx for unique identifier
    private int current_proposal_number;
    private String maxVal;

    // socket purpose
    private ArrayList<HashMap<String, String>> sitesInfo;
    int sendPort;
    DatagramSocket sendSocket;

    private String reservation; // from user command insert
    private int next_log_slot; // from user command insert

    // blocking queue: for receiving promise and ack
    private BlockingQueue blocking_queue = null;
    // siteIp -> pair(accNum, accVal)
    private HashMap<String, Map.Entry<Integer, String>> promise_queues;
    // siteIp -> reservation(string form)
    // FIXME: why reservation?
    private HashMap<String, String> ack_queues;
    // slot -> accVal
    private HashMap<Integer, String> learnt_slots;


    // ------------------CONSTRUCTOR------------------ //
    public Proposer(int uid, ArrayList<HashMap<String, String>> sitesInfo, DatagramSocket sendSocket, BlockingQueue queue) {
        this.uid = uid;
        this.current_proposal_number = uid;

        this.sitesInfo = sitesInfo;
        this.sendPort = Integer.parseInt(sitesInfo.get(uid).get("startPort"));
        this.sendSocket = sendSocket;

        this.next_log_slot = 0;

        this.blocking_queue = queue;
        this.promise_queues = new HashMap<>();
        this.ack_queues = new HashMap<>();
        this.learnt_slots = new HashMap<>();
    }


    // ------------------HELPERS------------------ //
    // after user input: reserve or cancel, start to propose a log slot
    // return: 1: successful propose -1: accVal != reserve  0: accNum not big enough
    public int propose() {
        // 1. send prepare for proposing
        this.sendPrepare();

        // define majority sites count
        int numSites = this.sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0); // FIXME: Am I right?

        // 2. blocking on receiving promise
        int success = 0; // 1: successful propose -1: accVal != reserve  0: accNum not big enough
        int maxAccNum = -1;
        String maxVal = null;
        // time out after 10000 millis
        long startTime = System.currentTimeMillis();
        while (success != 1 && (System.currentTimeMillis() - startTime) < 10000) {
            // FIXME: check poll method
            String curMsg = (String) this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("promise")) {
                // keep a record of every received promise in promise queue
                // format: siteIp -> pair(accNum, accVal)
                recvPromise(curMsg);
                int numPromise = this.promise_queues.size();
                if (numPromise >= majority) {
                    // try to choose largest accNum and accVal to send
                    for (Map.Entry<String, Map.Entry<Integer, String>> mapElement : this.promise_queues.entrySet()) {
                        Map.Entry<Integer, String> accEntry = mapElement.getValue();
                        int curAccNum = accEntry.getKey();
                        String curAccString = accEntry.getValue();
                        if (curAccNum > maxAccNum) {
                            maxVal = curAccString;
                        }
                    }
                    // choose my own proposal value to send
                    if (maxVal == null) {
                        // indicating successful proposal
                        success = 1;
                        maxVal = this.reservation;
                    }
                    // if my own reservation is dumped, my proposal is failed
                    // and keep on looking for the next slot
                    else {
                        this.maxVal = maxVal;
                        success = -1;
                        break;
                    }
                }
            } else if (splitted[0].equals("nack")) {
                recvNack(curMsg);
            }
            // when receiving commit for other slots
            else if (splitted[0].equals("commit")) {
                recvCommit(curMsg);
            }
        }
        // if timeout, return 0 to main and retry
        if (success != 1) return success;

            // 3. send accept, if receiving promise from majority
        else {
            sendAccept(this.current_proposal_number, maxVal);
        }

        // 4. blocking on receiving ack
        success = 0;
        startTime = System.currentTimeMillis();
        while (success != 1 && (System.currentTimeMillis() - startTime) < 10000) {
            String curMsg = (String) this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("ack")) {
                recvAck(curMsg);
                int numAck = this.ack_queues.size();
                if (numAck >= majority) {
                    success = 1;
                }
            } else if (splitted[0].equals("nack")) {
                recvNack(curMsg);
            }
            // when receiving commit for other slots
            else if (splitted[0].equals("commit")) {
                recvCommit(curMsg);
            }
        }
        // if timeout, return 0 to main and retry
        if (success != 1) return success;
        // successfully proposed, learn the chosen slot
        this.learnt_slots.put(this.next_log_slot, maxVal);

        // 5. change role to learner, send commit
        sendCommit(maxVal);

        return success;
    }


    public void sendPrepare() {
        // increment the proposal number
        this.current_proposal_number += 10;

        // generate message for sending: "prepare curPropNum next_log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(this.current_proposal_number);
        sb.append(" ");
        sb.append(this.next_log_slot);

        // send prepare to all sites
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            // FIXME: am I really sending
            Send prepare = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
            prepare.start();
        }

        System.err.println("sending prepare(" + this.current_proposal_number + ")to all sites");
    }


    // FIXME: don't need log_slot in promise message
    // promise form "Promise accNum accVal log_slot sender_ip"
    public void recvPromise(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int accNum = 0;
        String accVal = null;
        if (!splitted[1].equals("null")) {
            accNum = Integer.parseInt(splitted[1]);
            accVal = splitted[2];
        }
        String sender_ip = splitted[4];

        // store in my promise queue for current log slot
        // promise queues: slot_queue(siteIp -> pair(accNum, accVal)
        Map.Entry<Integer, String> recvAccs = new AbstractMap.SimpleEntry<Integer, String>(accNum, accVal);
        this.promise_queues.put(sender_ip, recvAccs);
    }


    // message form: accept proposalNum reservation(string form) logSlot
    public void sendAccept(int proposalNum, String reservation) {
        // build the accept message to send
        StringBuilder sb = new StringBuilder();
        sb.append("accept ");
        sb.append(proposalNum);
        sb.append(" ");
        sb.append(reservation);
        sb.append(" ");
        sb.append(this.next_log_slot);

        // send accept to the same set of majority
        for (Map.Entry<String, Map.Entry<Integer, String>> mapElement : this.promise_queues.entrySet()) {
            String recvIp = mapElement.getKey();
            for (int i = 0; i < this.sitesInfo.size(); i++) {
                if (this.sitesInfo.get(i).get("ip").equals(recvIp)) {
                    Send accept = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
                    accept.start();
                    break;
                }
            }

        }

        System.err.println("sending accept(" + proposalNum + "," + "'" + reservation + "') to same majority sites");
    }


    // message form: ack maxPrepNum senderIp
    public void recvAck(String message) {
        // keep a record of ack message
        // ack queue: slot -> slot_queue(siteIp -> reservation(string form))
        // ack would always be dealing with self proposal
        // FIXME: is reservation needed?
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
            Send commit = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
            commit.start();
        }

        System.err.println("sending commit ('" + accVal + "')to all sites except self");
    }


    // message form: commit accVal logSlot senderIP
    public void recvCommit(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int logSlot = Integer.parseInt(splitted[2]);
        String accVal = splitted[1];
        this.learnt_slots.put(logSlot, accVal);
    }


    public void recvNack(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int recvMaxNum = Integer.parseInt(splitted[1]);
        this.current_proposal_number = Math.max(recvMaxNum, this.current_proposal_number);
    }


    public HashMap<Integer, String> learn(HashMap<Integer, String> curLog) {
        for (Map.Entry<Integer, String> mapElement : this.learnt_slots.entrySet()) {
            int curSlot = mapElement.getKey();
            String curResv = mapElement.getValue();
            curLog.put(curSlot, curResv);
        }
        return curLog;
    }


    // ------------------GETTERS AND SETTERS------------------ //
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

    public String getMaxVal() {
        return maxVal;
    }

    public void setMaxVal(String maxVal) {
        this.maxVal = maxVal;
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

    public BlockingQueue getBlocking_queue() {
        return blocking_queue;
    }

    public void setBlocking_queue(BlockingQueue blocking_queue) {
        this.blocking_queue = blocking_queue;
    }

    public HashMap<String, Map.Entry<Integer, String>> getPromise_queues() {
        return promise_queues;
    }

    public void setPromise_queues(HashMap<String, Map.Entry<Integer, String>> promise_queues) {
        this.promise_queues = promise_queues;
    }

    public HashMap<String, String> getAck_queues() {
        return ack_queues;
    }

    public void setAck_queues(HashMap<String, String> ack_queues) {
        this.ack_queues = ack_queues;
    }

    public HashMap<Integer, String> getLearnt_slots() {
        return learnt_slots;
    }

    public void setLearnt_slots(HashMap<Integer, String> learnt_slots) {
        this.learnt_slots = learnt_slots;
    }

//    public ArrayList<Integer> getCommitted_slots() {
//        return committed_slots;
//    }
//
//    public void setCommitted_slots(ArrayList<Integer> committed_slots) {
//        this.committed_slots = committed_slots;
//    }
}
