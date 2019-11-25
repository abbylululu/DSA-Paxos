import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Proposer {
    // ------------------MEMBER VARS------------------ //
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
    DatagramSocket sendSocket; // send
    BlockingQueue queue; // receive

    private int current_proposal_number;
    private int current_log_slot;
    private String current_proposal_val; // from user input
    private String prev_proposal_val; // from user input

    private BlockingQueue blocking_queue = null;
    private HashMap<String, Map.Entry<Integer, String>> promise_queues;
    private HashMap<String, String> ack_queues;

    // ------------------CONSTRUCTOR------------------ //

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
        return false;
    }


    public int majority() {
        int numSites = this.sitesInfo.size();
        int majority = (int) Math.ceil(numSites / 2.0);
        return majority;
    }


    public void sendPrepare() {
        // increment the proposal number
        this.current_proposal_number += this.sitesInfo.size();

        // generate message for sending: "prepare curPropNum next_log_slot"
        StringBuilder sb = new StringBuilder();
        sb.append("prepare ");
        sb.append(this.current_proposal_number);
        sb.append(" ");
        sb.append(this.current_log_slot);

        // send prepare to all sites
        for (int i = 0; i < this.sitesInfo.size(); i++) {
            String recvIp = this.sitesInfo.get(i).get("ip");
            // FIXME: am I really sending
            Send prepare = new Send(recvIp, Integer.parseInt(this.sitesInfo.get(i).get("startPort")), this.sendSocket, sb.toString());
            prepare.start();
        }

        System.err.println("sending prepare(" + this.current_proposal_number + ")to all sites");
        System.out.println("Proposer<" + this.sitesInfo.get(uid).get("siteId") + "> sends prepare(" + this.current_proposal_number + ")to all sites");
    }



}