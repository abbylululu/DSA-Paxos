import javafx.util.Pair;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Proposer {
    private int _uid; // node Id for unique identifier
    private ArrayList<HashMap<String, String>> _sitesInfo;
    private int _current_proposal_number;
    private String _reservation; // from user command insert
    private int _next_log_slot; // from user command insert

    // blocking queue: for receiving promise and ack
    private Queue<String> _blocking_queue;
    // slot -> pair(curPropNum, Reservation(in string form))
    private HashMap<Integer, Pair<Integer, String>> _my_proposals;
    // siteIp -> pair(accNum, accVal)
    private HashMap<String, Pair<Integer, String>> _promise_queues;
    // slot -> slot_queue(siteIp -> reservation(string form))
    private HashMap<Integer, HashMap<String, String>> _ack_queues;
    private ArrayList<Integer> _committed_slots;

    public Proposer(int next_log_slot, String reservation, int uid, ArrayList<HashMap<String, String>> sitesInfo) {
        this._uid = uid;
        this._sitesInfo = sitesInfo;
        this._current_proposal_number = uid;
        this._reservation = reservation;
        this._next_log_slot = next_log_slot;

        this._blocking_queue = new LinkedList<>();
        this._my_proposals = new HashMap<>();
        this._promise_queues = new HashMap<>();
        this._ack_queues = new HashMap<>();
        this._committed_slots = new ArrayList<>();
    }

    public void sendPrepare() {
        // choose a proposal number to propose
        this._current_proposal_number += 10;
        int n = this._current_proposal_number;

        // keep a record of my proposals: slot -> proposalNum, reservation(string form)
        Pair<Integer, String> insertInfo = new Pair<>(n, this._reservation);
        this._my_proposals.put(this._next_log_slot, insertInfo);

        // generate message for sending: "prepare curPropNum next_log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(n);
        sb.append(" ");
        sb.append(this._next_log_slot);

        // TODO: sending the message in parallel
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
        this._promise_queues.put(sender_ip, recvAccs);
    }

    // message form: accept proposalNum reservation(string form)
    public void sendAccept(int proposalNum, String reservation, int log_slot) {
        // build the accept message to send
        StringBuilder sb = new StringBuilder();
        sb.append("accept ");
        sb.append(proposalNum);
        sb.append(" ");
        sb.append(reservation);

        // TODO send accept in parallel
    }

    // message form: ack maxPrepNum senderIp
    public void recvAck(String message) {
        // keep a record of ack message
        // ack queue: slot -> slot_queue(siteIp -> reservation(string form))
        // ack would always be dealing with selp proposal
        String curSiteIp = new String();
        for (int i = 0; i < this._sitesInfo.size(); i++) {
            if (this._sitesInfo.get(i).get("siteId").equals(this._uid)) {
                curSiteIp = this._sitesInfo.get(i).get("ip");
            }
        }
        this._ack_queues.get(this._next_log_slot).put(curSiteIp, this._reservation);
    }

    // after user input: reserve or cancel, start to propose a log slot
    // return: 1: successful propose 0: unsuccessful propose
    public int propose() {
        // 1. send prepare for proposing
        this.sendPrepare();

        // define majority sites count
        int numSites = this._sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0); // FIXME: Am I right?

        // 2. blocking on receiving promise
        // TODO: timeout
        int success = 0;
        while(!(this._blocking_queue).isEmpty()) {
            String curMsg = this._blocking_queue.poll();
            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("promise")) {
                // keep a record of every received promise in promise queue
                // format: siteIp -> pair(accNum, accVal)
                recvPromise(curMsg);
                int numPromise = this._promise_queues.size();
                if (numPromise >= majority) {
                    // indicating successful proposal
                    success = 1;
                    // try to choose largest accNum and accVal to send
                    int maxNum = 0;
                    String maxVAl;

                }
            }
        }
    }
}
