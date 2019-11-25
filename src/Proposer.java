import java.net.DatagramSocket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Proposer {
    // ------------------MEMBER VARS------------------ //
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
    private DatagramSocket sendSocket; // send
    private BlockingQueue<String> blocking_queue; // receive

    private int current_proposal_number;
    private int current_log_slot;
    private String current_proposal_val; // from user input

    private HashMap<String, Map.Entry<Integer, String>> promise_queues;
    private int ack_counter;

    // ------------------CONSTRUCTOR------------------ //


    public Proposer(int uid, ArrayList<HashMap<String, String>> sitesInfo, DatagramSocket sendSocket, BlockingQueue<String> blocking_queue) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
        this.sendSocket = sendSocket;
        this.blocking_queue = blocking_queue;
        this.promise_queues = new HashMap<>();
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
        while ((System.currentTimeMillis() - startTime) < 10000) {
            String curMsg = (String) this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("promise")) {
                recvPromise(curMsg);
                int numPromise = this.promise_queues.size();
                if (numPromise >= majority) {
                    for (Map.Entry<String, Map.Entry<Integer, String>> mapElement : this.promise_queues.entrySet()) {
                        Map.Entry<Integer, String> accEntry = mapElement.getValue();
                        int curAccNum = accEntry.getKey();
                        String curAccString = accEntry.getValue();
                        if (curAccNum > maxAccNum) {
                            maxVal = curAccString;
                        }
                    }
                    if (maxVal != null) {
                        this.current_proposal_val = maxVal;
                    }
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
        this.promise_queues.clear();
        this.ack_counter = 0;
    }


    public boolean synodPhase2() {
        int majority = this.majority();

        sendAccept(this.current_proposal_number, this.current_proposal_val);
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            String curMsg = (String) this.blocking_queue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("ack")) {
                this.ack_counter++;

                if (this.ack_counter >= majority) {
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


    public boolean startSynod(Integer logSlot, String val) {
        this.current_log_slot = logSlot;
        this.current_proposal_val = val;

        // from user input

        int cnt = 3;
        while(cnt > 0) {
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

        if (cnt <= 0) return false;

        // commit
        sendCommit(this.current_proposal_number, this.current_proposal_val);

        return this.current_proposal_val.equals(val);
    }


    public int majority() {
        int numSites = this.sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0);
        return majority;
    }


    public void sendPrepare() {
        // increment the proposal number
        this.current_proposal_number += this.sitesInfo.size();

        // generate message for sending: "prepare curPropNum log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(this.current_proposal_number);
        sb.append(" ");
        sb.append(this.current_log_slot);

        // send prepare to all sites
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send prepare = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
            prepare.start();
        }

//        System.err.println("sending prepare(" + this.current_proposal_number + ")to all sites");
        System.out.println("Proposer<" + this.sitesInfo.get(uid).get("siteId") + "> sends prepare(" + this.current_proposal_number + ")to all sites");
    }


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


    public void recvNack(String message) {
        // parse the received message
        String[] splitted = message.split(" ");
        int recvMaxNum = Integer.parseInt(splitted[1]);
        this.current_proposal_number = Math.max(recvMaxNum, this.current_proposal_number);
    }

    public void sendAccept(int proposalNumber, String proposalVal) {
        String msg = String.format("accept %d %s %d", proposalNumber,
                proposalVal, this.current_log_slot);

//        System.out.println("****accept from apple is: " + msg);

        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send accept = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, msg);
            accept.start();
        }

//        System.err.println("sending accept(" + proposalNumber + "," + "'" + proposalVal + "') to all sites");
        System.out.println("Proposer<" + this.sitesInfo.get(uid).get("siteId") + "> sends accept(" + proposalNumber + "," + "'" + proposalVal + "') to all sites");
    }


    public void sendCommit(int accNum, String accVal) {
        String msg = String.format("commit %d %s %d %s", accNum, accVal,
                this.current_log_slot, this.sitesInfo.get(uid).get("ip"));

        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            Send commit = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, msg);
            commit.start();
        }

//        System.err.println("sending commit ('" + accVal + "')to all sites");
        System.out.println("Distinguished Learner<" + this.sitesInfo.get(uid).get("siteId") + "> sends commit ('" + accVal + "')to all sites");
    }
}