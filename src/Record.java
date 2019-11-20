public class Record {
    private Integer logSlot;
    private Integer maxPrepare;
    private Integer accNum;
    private String accVal;

    public Record(Integer logSlot, Integer maxPrepare, Integer accNum, String accVal) {
        this.logSlot = logSlot;
        this.maxPrepare = maxPrepare;
        this.accNum = accNum;
        this.accVal = accVal;
    }
}
