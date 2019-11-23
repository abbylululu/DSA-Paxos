import java.io.Serializable;
import java.util.HashMap;

public class Record implements Serializable {
    private HashMap<Integer, Integer> maxPrepare;//log_slot to maxPrepare
    private HashMap<Integer, Integer> accNum;
    private HashMap<Integer, String> accVal;

    public Record(HashMap<Integer, Integer> maxPrepare, HashMap<Integer, Integer> accNum, HashMap<Integer, String> accVal) {
        this.maxPrepare = maxPrepare;
        this.accNum = accNum;
        this.accVal = accVal;
    }

    public HashMap<Integer, Integer> getMaxPrepare() {
        return maxPrepare;
    }

    public HashMap<Integer, Integer> getAccNum() {
        return accNum;
    }

    public HashMap<Integer, String> getAccVal() {
        return accVal;
    }
}
