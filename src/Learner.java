import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Learner extends Thread{
    private BlockingQueue<String> learnerQueue = null;
    private static ArrayList<Reservation> checkPointDictionary;
    static ArrayList<Reservation> dictionary; // local reservation data structure
    static TreeMap<Integer, Reservation> log; // array of reservation string(ops clientName 1 2 3), in stable storage
    private Proposer proposer = null;

    public Learner(BlockingQueue<String> learnerQueue, Proposer proposer) throws IOException, ClassNotFoundException {
        this.learnerQueue = learnerQueue;
        this.proposer = proposer;
        checkPointDictionary = new ArrayList<>();
        dictionary = new ArrayList<>();
        log = new TreeMap<>();
        File logFile = new File(Host.curSiteId + "log.txt");
        File dictFile = new File(Host.curSiteId +"dictionary.txt");
        if (dictFile.exists()) {
            recoverDict();
        }
        if (logFile.exists()) {
            recoverLog();
        }
        replay();
    }

    public void run() {
        while (true) {
            String curMsg = (String) this.learnerQueue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("commit")) {
                try {
                    recvCommit(curMsg);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean checkBack(Reservation newRecord, Integer logSlot) {
        Integer maxLogNum = getMaxLogSlot();
        for (int i = logSlot; i <= maxLogNum; i++) {
            if (log.containsKey(i)) {
                Reservation compare = log.get(i);
                if (newRecord.getOperation().equals("reserve") && compare.getOperation().equals("cancel") &&
                newRecord.getClientName().equals(compare.getClientName())) return true;

                if (newRecord.getOperation().equals("cancel") && compare.getOperation().equals("reserve") &&
                newRecord.getClientName().equals(compare.getClientName())) return true;
            }
        }
        return false;
    }

    // message form: commit accVal logSlot senderIP
    public void recvCommit(String message) throws IOException {
        // parse the received message
        //"commit accNum accVal logSlot senderIP"
        //
        String[] splitted = message.split(" ");
        int msgLen = splitted.length;
        String accNum = splitted[1].trim();
        String logSlot = splitted[msgLen - 2].trim();
        String operation = splitted[2].trim();
        String clientName = splitted[3].trim();
        String sendIp = splitted[msgLen - 1].trim();
        ArrayList<Integer> flights = new ArrayList<>();
        String accVal = operation + " " + clientName + " ";
        for (int i = 4; i < msgLen - 2; i++) {
            accVal += splitted[i] + " ";
            flights.add(Integer.parseInt(splitted[i]));
        }
        Reservation record = new Reservation(operation.trim(), clientName.trim(), flights);
        record.setProposerIp(Acceptor.proposerIp.get(Integer.parseInt(logSlot)));
        if (!Learner.log.containsKey(Integer.parseInt(logSlot))) {
            record.setPrintString(accVal.trim());
            addLog(Integer.parseInt(logSlot), record, this.proposer);// update log
        }
        if (checkBack(record, Integer.parseInt(logSlot))) return;
        if (operation.equals("reserve")) {
            boolean add = true;
            for (int i = 0; i < Learner.dictionary.size(); i++) {
                if (Learner.dictionary.get(i).flatten().equals(record.flatten())) {
                    add = false;
                }
            }
            if (add) {
                Learner.dictionary.add(record);
            }
        } else {
            for (int i = 0; i < Learner.dictionary.size();) {
                if (Learner.dictionary.get(i).getClientName().equals(clientName)) {
                    Learner.dictionary.remove(Learner.dictionary.get(i));
                } else {
                    i++;
                }
            }
        }
    }


    public Integer findLastCheck() {
        for(Map.Entry<Integer,Reservation> entry : Learner.log.descendingMap().entrySet()) {
            Reservation value = entry.getValue();
            if (value.isCheckPoint()) {
                return entry.getKey();
            }
        }
        return -1;// shouldn't be here
    }

    public void replay() {
        if (Learner.log.isEmpty()) return;
        Integer startPoint = findLastCheck();
        Integer maxPoint = getMaxLogSlot();
        // Replay log from startPoint to maxPoint
        for (int i = startPoint; i <= maxPoint; i++) {
            if (Learner.log.containsKey(i)) {
                if (Learner.log.get(i).getOperation().equals("reserve")) {// add
                    boolean add = true;
                    for (int j = 0; j < Learner.dictionary.size(); j++) {
                        if (Learner.dictionary.get(j).flatten().equals(Learner.log.get(i).flatten())) {
                            add = false;
                        }
                    }
                    if (add) {
                        Learner.dictionary.add(Learner.log.get(i));
                    }

                } else {// cancel
                    for (int j = 0; j < Learner.dictionary.size();) {
                        if (Learner.dictionary.get(j).getClientName().equals(Learner.log.get(i).getClientName())) {
                            Learner.dictionary.remove(Learner.dictionary.get(j));
                        } else {
                            j++;
                        }
                    }
                }
            }
        }
    }
    

    public static void addLog(Integer logSlot, Reservation logRecord, Proposer proposer) throws IOException {
        Integer curMax = getMaxLogSlot();
        if (curMax / 5 != logSlot / 5) {// learn hole
            Host.learnHole(proposer);
        }
        if (logSlot % 5 == 0) logRecord.setCheckPoint(true);
        Learner.log.put(logSlot, logRecord);
        storeLog();
        if (logSlot % 5 == 0) storeDict();
    }

    public static Integer getMaxLogSlot() {
        Set<Integer> logSlot= Learner.log.keySet();
        Integer maxLog = 0;
        for (Integer num: logSlot) {
            if (num > maxLog) maxLog = num;
        }
        return maxLog;
    }


    public static void storeLog() throws IOException {
        byte[] output = Send.serialize(log);
        File file = new File(Host.curSiteId + "log.txt");
        FileOutputStream fos = null;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    public static void storeDict() throws IOException {
        byte[] output = Send.serialize(checkPointDictionary);
        File file = new File(Host.curSiteId +"dictionary.txt");
        FileOutputStream fos = null;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }


    private void recoverLog() throws IOException, ClassNotFoundException {
        @SuppressWarnings (value="unchecked")
        TreeMap<Integer, Reservation> recoverLog =
                (TreeMap<Integer, Reservation>)Acceptor.deserialize(Acceptor.readFromFile(Host.curSiteId +"log.txt"));
        Learner.log = recoverLog;
    }

    private void recoverDict() throws IOException, ClassNotFoundException {
        @SuppressWarnings (value="unchecked")
        ArrayList<Reservation> recoverDict =
                (ArrayList<Reservation>)Acceptor.deserialize(Acceptor.readFromFile(Host.curSiteId +"dictionary.txt"));
        Learner.dictionary = recoverDict;
    }
}
