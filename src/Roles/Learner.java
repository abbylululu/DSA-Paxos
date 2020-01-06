package Roles;

import Messages.Reservation;
import Utils.SendUtils;
import App.Host;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Learner extends Thread {
    private BlockingQueue<String> learnerQueue;
    private static ArrayList<Reservation> checkPointDictionary;
    // local reservation data structure
    public static ArrayList<Reservation> dictionary;
    // array of reservation string(ops clientName 1 2 3), in stable storage
    public static TreeMap<Integer, Reservation> log;
    private Proposer proposer;
    static Integer newMax;

    public Learner(BlockingQueue<String> learnerQueue, Proposer proposer,
                   @NotNull ArrayList<HashMap<String, String>> sitesInfo,
                   DatagramSocket sendSocket) throws IOException, ClassNotFoundException {
        this.learnerQueue = learnerQueue;
        this.proposer = proposer;
        checkPointDictionary = new ArrayList<>();
        dictionary = new ArrayList<>();
        log = new TreeMap<>();
        newMax = -1;
        File logFile = new File(Host.curSiteId + "log.txt");
        File dictFile = new File(Host.curSiteId + "dictionary.txt");
        String askMax = "MaximumLog";
        for (HashMap<String, String> stringStringHashMap : sitesInfo) {
            String recvIp = stringStringHashMap.get("ip");
            SendUtils ask = new SendUtils(recvIp, Integer.parseInt(stringStringHashMap.get("startPort")), sendSocket, askMax);
            ask.start();
        }
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
            String curMsg = this.learnerQueue.poll();
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

    private void recvCommit(@NotNull String message) throws IOException {
        // parse the received message
        //message form: "commit accNum accVal logSlot senderIP"
        String[] splitted = message.split(" ");
        int msgLen = splitted.length;
        String accNum = splitted[1].trim();
        String logSlot = splitted[msgLen - 2].trim();
        String operation = splitted[2].trim();
        String clientName = splitted[3].trim();
        String sendIp = splitted[msgLen - 1].trim();
        ArrayList<Integer> flights = new ArrayList<>();
        StringBuilder accVal = new StringBuilder(operation + " " + clientName + " ");
        for (int i = 4; i < msgLen - 2; i++) {
            accVal.append(splitted[i]).append(" ");
            flights.add(Integer.parseInt(splitted[i]));
        }
        Reservation record = new Reservation(operation.trim(), clientName.trim(), flights);
        record.setProposerIp(Acceptor.proposerIp.get(Integer.parseInt(logSlot)));
        if (!Learner.log.containsKey(Integer.parseInt(logSlot))) {
            record.setPrintString(accVal.toString().trim());
            if (record.getPrintString().isEmpty()) System.err.println("Empty");
            // update log
            addLog(Integer.parseInt(logSlot), record, this.proposer);
        }
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
            for (int i = 0; i < Learner.dictionary.size(); ) {
                if (Learner.dictionary.get(i).getClientName().equals(clientName)) {
                    Learner.dictionary.remove(Learner.dictionary.get(i));
                } else {
                    i++;
                }
            }
        }
    }


    private Integer findLastCheck() {
        for (Map.Entry<Integer, Reservation> entry : Learner.log.descendingMap().entrySet()) {
            Reservation value = entry.getValue();
            if (value.isCheckPoint()) {
                return entry.getKey();
            }
        }
        return -1;
    }

    private void replay() {
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

                    // cancel
                } else {
                    for (int j = 0; j < Learner.dictionary.size(); ) {
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


    private static void addLog(Integer logSlot, Reservation logRecord, Proposer proposer) throws IOException {
        Integer curMax = getMaxLogSlot();
        // learn hole
        if (curMax / 5 != logSlot / 5) {
            Host.learnHole(proposer);
        }
        if (logSlot % 5 == 0) logRecord.setCheckPoint(true);
        Learner.log.put(logSlot, logRecord);
        storeLog();
        if (logSlot % 5 == 0) storeDict();
    }

    @Contract(pure = true)
    public static Integer getMaxLogSlot() {
        Set<Integer> logSlot = Learner.log.keySet();
        Integer maxLog = 0;
        for (Integer num : logSlot) {
            if (num > maxLog) maxLog = num;
        }
        return maxLog;
    }


    private static void storeLog() throws IOException {
        byte[] output = SendUtils.serialize(log);
        File file = new File(Host.curSiteId + "log.txt");
        FileOutputStream fos;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    private static void storeDict() throws IOException {
        byte[] output = SendUtils.serialize(checkPointDictionary);
        File file = new File(Host.curSiteId + "dictionary.txt");
        FileOutputStream fos;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }


    private void recoverLog() throws IOException, ClassNotFoundException {
        @SuppressWarnings(value = "unchecked")
        TreeMap<Integer, Reservation> recoverLog =
                (TreeMap<Integer, Reservation>) Acceptor.deserialize(Acceptor.readFromFile(Host.curSiteId + "log.txt"));
        Learner.log = recoverLog;
        Host.learnBackHole(this.proposer, newMax);
        newMax = -1;
    }

    private void recoverDict() throws IOException, ClassNotFoundException {
        @SuppressWarnings(value = "unchecked")
        ArrayList<Reservation> recoverDict =
                (ArrayList<Reservation>) Acceptor.deserialize(Acceptor.readFromFile(Host.curSiteId + "dictionary.txt"));
        Learner.dictionary = recoverDict;
    }
}
