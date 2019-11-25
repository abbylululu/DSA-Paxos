import java.io.*;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class ReservationSys {
    // ------------------MEMBER VARS------------------ //
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
//    private ArrayList<Reservation> dictionary; // local reservation data structure
//    private HashMap<Integer, String> log; // array of reservation string(ops clientName 1 2 3), in stable storage
    private Proposer proposer;


    // ------------------CONSTRUCTOR------------------ //
    public ReservationSys(ArrayList<HashMap<String, String>> sitesInfo, int uid, DatagramSocket sendSocket, BlockingQueue queue) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
//        this.dictionary = new ArrayList<>();
//        this.log = new HashMap<>();
        this.proposer = new Proposer(uid, this.sitesInfo, sendSocket, queue);
    }


    // ------------------HELPERS------------------ //
    // -1: conflicted. 0: not added. 1: success
    public int insert (String[] orderInfo) {
        // get the maxSlot index of current log
        int maxSlot = -1;
        if (!Learner.log.isEmpty()) {
            // offset potential holes
            for (Map.Entry<Integer, String> mapElement: Learner.log.entrySet()) {
                maxSlot = Math.max(maxSlot, mapElement.getKey());
            }

            int offsetRet = offsetHole(maxSlot);
            if (offsetRet == 0) {
                // TODO: learn hole failed
            }
        }

        // 1. detect conflict
        String clientName = orderInfo[1];
        ArrayList<Integer> flights = new ArrayList<>();
        for (String s : orderInfo[2].split(",")) {
            flights.add(Integer.parseInt(s));
        }
        if (isConflict(flights)) {return -1;}

        Reservation newResv = new Reservation("reserve", clientName, flights);
        String reservation = newResv.flatten();


        // 3. propose for the chosen slot
        return proposeChosenSlot(maxSlot + 1, reservation);
    }


    // -1: deleted before. 0: not added. 1: success
    public int delete (String[] orderInfo) {
        // get the maxSlot index of current log
        int maxSlot = -1;
        if (!Learner.log.isEmpty()) {
            // offset hole in log
            for (Map.Entry<Integer, String> mapElement : Learner.log.entrySet()) {
                maxSlot = Math.max(maxSlot, mapElement.getKey());
            }

            int offsetRet = offsetHole(maxSlot);
            if (offsetRet == 0) {
                // TODO: learn hole failed
            }
        }

        String clientName = orderInfo[1];
        String cancel = "cancel " + clientName;

        // 2. check whether it has been deleted before
        for (Map.Entry<Integer, String> mapElement: Learner.log.entrySet()) {
            if (cancel.equals(mapElement.getValue())) {
                // cancel request failed
                return -1;
            }
        }

        // 3. propose for the chosen slot
        return proposeChosenSlot(maxSlot + 1, cancel);
    }


    public boolean isConflict(ArrayList<Integer> flights) {
        // keep track of reserved flights in dict, mapping flight number to count
        HashMap<Integer, Integer> ReservedFlights = new HashMap<>();
        if (Learner.dictionary.isEmpty()) return false;
        for (int i = 0; i < Learner.dictionary.size(); i++) {
            ArrayList<Integer> curLocalReservedFlights = Learner.dictionary.get(i).getFlights();
            for (int j = 0; j < curLocalReservedFlights.size(); j++) {
                Integer curFlight = curLocalReservedFlights.get(j);
                if (ReservedFlights.containsKey(curFlight)) {
                    Integer curCnt = ReservedFlights.get(curFlight);
                    ReservedFlights.put(curFlight, curCnt + 1);
                } else {
                    ReservedFlights.put(curFlight, 1);
                }
            }
        }

        for (int i = 0; i < flights.size(); i++) {
            if (ReservedFlights.containsKey(flights.get(i)) && ReservedFlights.get(flights.get(i)) == 2) {
                return true;
            }
        }
//        for (int i = 0; i < Learner.dictionary.size(); i++) {
//            // reserved flights for each reservation provided in local dictionary
//            ArrayList<Integer> curLocalReservedFlights = Learner.dictionary.get(i).getFlights();
//            for (int j = 0; j < curLocalReservedFlights.size(); j++) {
//                Integer curFlight = curLocalReservedFlights.get(j);
//                // update current flight counts in dictionary
//                Integer curCnt = ReservedFlights.get(curFlight); // current flight counts in dictionary
//                ReservedFlights.put(curFlight, (curCnt == null) ? 1 : curCnt + 1);
//            }
//        }
//
//        for (int i = 0; i < flights.size(); i++) {
//            int flight = flights.get(i);
//            // if there exists a flight that is fully booked in log, that is a conflict
//            if (!ReservedFlights.isEmpty() && ReservedFlights.get(flight) != null) {
//                if (ReservedFlights.get(flight) == 2) {
//                    return true;
//                }
//            }
//        }
        return false;
    }


    public void updateDict() {
        for (Map.Entry<Integer, String> mapElement: Learner.log.entrySet()) {
            if (mapElement.getValue() == null) continue;
            String curEntry = mapElement.getValue();

            String[] splitted = curEntry.split(" ");
            String operation = splitted[0];

//            System.out.println("***size before: " + this.dictionary.size());
//            for (int j = 0; j < this.dictionary.size(); j++) {
//                System.out.println("*** " + this.dictionary.get(j).flatten());
//            }

            // insert
            if (operation.equals("reserve")) {
                Reservation curResvObj = new Reservation(curEntry);
//                System.out.println("####in log: " + curEntry);
//                System.out.println("####changed to obj: " + curResvObj.flatten());
//                if (!this.dictionary.isEmpty()) System.out.println("####in dict: " + this.dictionary.get(0).flatten());
//                System.out.println("&&&&&&" + curResvObj.equals(this.dictionary.get(0)));
                boolean dups = false;
                for (int i = 0; i < Learner.dictionary.size(); i++) {
                    if (Learner.dictionary.get(i).flatten().equals(curEntry)) {
                        dups = true;
                    }
                }
                if (dups) continue;
//                if (this.dictionary.contains(curResvObj)) continue;
//                System.out.println("***&&&");
                Learner.dictionary.add(curResvObj);
            }
            // delete
            else if (operation.equals("cancel")) {
                String clientName = splitted[1];
                System.out.println("***log entry cancle client: " + clientName);

                for(int i = 0; i < Learner.dictionary.size(); i++) {
                    if (Learner.dictionary.get(i).getClientName().equals(clientName)) {
                        System.out.println("***deleting record: " + Learner.dictionary.get(i).flatten());

                        Learner.dictionary.remove(Learner.dictionary.get(i));

                        System.out.println("***size after: " + Learner.dictionary.size());
                    }
                }
            }
        }
    }


    public int offsetHole(int maxSlot) {
        for (int i = 0; i <= maxSlot; i++) {
            // TODO: check key first then value next
            // there is a hole in local log
            if (Learner.log.get(i) == null) {
                System.out.println("hello????");
                int cnt = 3;
                while (cnt > 0) {
                    this.proposer.setNext_log_slot(i);
                    this.proposer.setReservation("");
                    int proposeRet = proposer.propose();
                    // if successfully get back the lost information
                    if (proposeRet == -1) {
                        break;
                    }
                    cnt--;
                }
                // successfully learnt hole
                if (cnt > 0) {
                    Learner.log.put(i, proposer.getMaxVal());
                    this.store();
                    // update local dict
                    this.updateDict();
                }
                // there is a hole that is failed to fill
                else {
                    return 0;
                }
            }
        }
        return 1;
    }


    // 0: not adding to the log  1: successfully proposed
    public int proposeChosenSlot(int maxSlot, String reservation) {
        this.proposer.setNext_log_slot(maxSlot);
        this.proposer.setReservation(reservation);
        int cnt = 3;
        while (cnt > 0) {
            int proposeRet = proposer.propose();
            // if successfully proposed
            if (proposeRet == 1) break;
                // if failed in the competing of the chosen slot, need to propose for the next slot
            else if(proposeRet == -1) {
                System.out.println("hello???????");
                // store information for current chosen slot
                Learner.log.put(this.proposer.getNext_log_slot(), proposer.getMaxVal());
                this.store();
                // update local dict
                this.updateDict();
                // propose for the next slot
                this.proposer.setNext_log_slot(this.proposer.getNext_log_slot() + 1);
            }
            cnt--;
        }
        // successfully proposed
        if (cnt > 0) {
            // update log entry
            Learner.log = this.proposer.learn(Learner.log);
            this.store();
            // update local dict
            this.updateDict();
            return 1;
        }
        else {
            // not adding to the log
            return 0;
        }
    }


    // format: slot,ops clientName (1 2 3)
    public void store() {
        // logSlot accVal senderIp
        for (Map.Entry<Integer, String> mapElement: Learner.log.entrySet()) {
            // empty log slot
            if (mapElement.getValue() == null) continue;
            String log = mapElement.getKey() + "," + mapElement.getValue();
            try {
                // FIXME: am I really writing?
                BufferedWriter writer = new BufferedWriter(new FileWriter("Log.txt"));
                writer.write(log);
                writer.close();
            } catch (IOException e) {
                System.out.println("write exception");
            }
        }
    }


    public void recover() throws IOException {
        BufferedReader logReader = new BufferedReader(new FileReader("Log.txt"));
        String line = "";
        while ((line = logReader.readLine()) != null) {
            String[] splitted = line.split(",");
            int curSlot = Integer.parseInt(splitted[0]);
            String curLogEntry = splitted[1];
            Learner.log.put(curSlot, curLogEntry);
        }
        this.updateDict();
    }


    void printLog() {
        for (int i = 0; i < Learner.log.size(); i++) {
            System.out.println(Learner.log.get(i));
        }
    }

    void printDictionary() {
        ArrayList<Reservation> newDict = new ArrayList<>();
        newDict = Learner.dictionary;
        for (int i = 0; i < newDict.size(); i++) {
            newDict.sort(new CustomComparator());
            System.out.println(newDict.get(i).flatten());
        }
    }

    // ------------------GETTERS AND SETTERS------------------ //

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public ArrayList<HashMap<String, String>> getSitesInfo() {
        return sitesInfo;
    }

    public void setSitesInfo(ArrayList<HashMap<String, String>> sitesInfo) {
        this.sitesInfo = sitesInfo;
    }

    public ArrayList<Reservation> getDictionary() {
        return Learner.dictionary;
    }

    public void setDictionary(ArrayList<Reservation> dictionary) {
        Learner.dictionary = dictionary;
    }

    public HashMap<Integer, String> getLog() {
        return Learner.log;
    }

    public void setLog(HashMap<Integer, String> log) {
        Learner.log = log;
    }

    public Proposer getProposer() {
        return proposer;
    }

    public void setProposer(Proposer proposer) {
        this.proposer = proposer;
    }
}


class CustomComparator implements Comparator<Reservation> {
    @Override
    public int compare(Reservation o1, Reservation o2) {
        return o1.getClientName().compareTo(o2.getClientName());
    }
}