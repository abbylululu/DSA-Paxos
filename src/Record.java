/*
* This data structure is for stable storage of Acceptor
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.TreeMap;

public class Record implements Serializable {
    private TreeMap<Integer, Integer> maxPrepare;//log_slot to maxPrepare
    private TreeMap<Integer, Integer> accNum;
    private TreeMap<Integer, String> accVal;
    private TreeMap<Integer, String> proposerIp;
    private HashMap<String, String> lastSeen;

    public Record(TreeMap<Integer, Integer> maxPrepare, TreeMap<Integer, Integer> accNum,
                  TreeMap<Integer, String> accVal, TreeMap<Integer, String> proposerIp) {
        this.maxPrepare = maxPrepare;
        this.accNum = accNum;
        this.accVal = accVal;
        this.proposerIp = proposerIp;
        this.lastSeen = new HashMap<>();
    }

    public TreeMap<Integer, Integer> getMaxPrepare() {
        return maxPrepare;
    }

    public TreeMap<Integer, Integer> getAccNum() {
        return accNum;
    }

    public TreeMap<Integer, String> getAccVal() {
        return accVal;
    }

    public TreeMap<Integer, String> getProposerIp() {
        return proposerIp;
    }

    public HashMap<String, String> getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(HashMap<String, String> lastSeen) {
        this.lastSeen = lastSeen;
    }
}
