import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Learner extends Thread{
    private BlockingQueue<String> learnerQueue = null;
    static ArrayList<Reservation> dictionary; // local reservation data structure
    static TreeMap<Integer, Reservation> log; // array of reservation string(ops clientName 1 2 3), in stable storage
    private Proposer proposer = null;

    public Learner(BlockingQueue<String> learnerQueue, Proposer proposer) throws IOException, ClassNotFoundException {
        this.learnerQueue = learnerQueue;
        this.proposer = proposer;
        dictionary = new ArrayList<>();
        log = new TreeMap<>();
        File logFile = new File("log.txt");
        File dictFile = new File("dictionary.txt");
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

    // message form: commit accVal logSlot senderIP
    public void recvCommit(String message) throws IOException {
        // parse the received message
        //"commit accNum accVal logSlot senderIP"
        //
        String[] splitted = message.split(" ");
        int msgLen = splitted.length;
        String accNum = splitted[1];
        String logSlot = splitted[msgLen - 2];
        String operation = splitted[2];
        String clientName = splitted[3];
        ArrayList<Integer> flights = new ArrayList<>();
        String accVal = operation + " " + clientName + " ";
        for (int i = 4; i < msgLen - 2; i++) {
            accVal += splitted[i] + " ";
            flights.add(Integer.parseInt(splitted[i]));
        }
        if (!Learner.log.containsKey(Integer.parseInt(logSlot))) {
            Reservation logRecord = new Reservation(operation, clientName, splitted[msgLen - 1], flights);
            logRecord.setPrintString(accVal.toString().trim());
            addLog(Integer.parseInt(logSlot), logRecord, this.proposer);// update log
        }

        if (operation.equals("reserve")) {
            Reservation record = new Reservation(operation, clientName, splitted[msgLen - 1], flights);
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
                        if (Learner.dictionary.get(j).getClientName().equals(Learner.log.get(i))) {
                            Learner.dictionary.remove(Learner.dictionary.get(i));
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
        File file = new File("log.txt");
        FileOutputStream fos = null;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    public static void storeDict() throws IOException {
        byte[] output = Send.serialize(dictionary);
        File file = new File("dictionary.txt");
        FileOutputStream fos = null;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }


    private void recoverLog() throws IOException, ClassNotFoundException {
        @SuppressWarnings (value="unchecked")
        TreeMap<Integer, Reservation> recoverLog =
                (TreeMap<Integer, Reservation>)Acceptor.deserialize(Acceptor.readFromFile("log.txt"));
        Learner.log = recoverLog;
    }

    private void recoverDict() throws IOException, ClassNotFoundException {
        @SuppressWarnings (value="unchecked")
        ArrayList<Reservation> recoverDict =
                (ArrayList<Reservation>)Acceptor.deserialize(Acceptor.readFromFile("dictionary.txt"));
        Learner.dictionary = recoverDict;
    }
}
