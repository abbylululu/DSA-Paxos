import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class Learner extends Thread{
    private BlockingQueue learnerQueue = null;
    static ArrayList<Reservation> dictionary; // local reservation data structure
    static HashMap<Integer, String> log; // array of reservation string(ops clientName 1 2 3), in stable storage

    public Learner(BlockingQueue learnerQueue) {
        this.learnerQueue = learnerQueue;
        dictionary = new ArrayList<>();
        log = new HashMap<>();
    }

    public void run() {
        while (true) {
            String curMsg = (String) this.learnerQueue.poll();
            if (curMsg == null) continue;

            String[] splitted = curMsg.split(" ");
            if (splitted[0].equals("commit")) {
                recvCommit(curMsg);
            }
        }
    }

    // message form: commit accVal logSlot senderIP
    public void recvCommit(String message) {
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
            Learner.log.put(Integer.parseInt(logSlot), accVal.toString());// update log
        }

        if (operation.equals("reserve")) {
            Reservation record = new Reservation(operation, clientName, flights);
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
}
