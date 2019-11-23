import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ReservationSys {
    private int uid;
    private ArrayList<HashMap<String, String>> sitesInfo;
    private ArrayList<Reservation> dictionary; // local reservation data structure
    // FIXME: object or string
    private HashMap<Integer, Reservation> log; // array of reservation object, in stable storage
    private Proposer proposer;

    public ReservationSys(ArrayList<HashMap<String, String>> sitesInfo, int uid, DatagramSocket sendSocket, BlockingQueue queue) {
        this.uid = uid;
        this.sitesInfo = sitesInfo;
        this.dictionary = new ArrayList<>();
        this.log = new HashMap<>();
        this.proposer = new Proposer(uid, this.sitesInfo, sendSocket, queue);
    }

    public void store() {
        // logSlot accVal senderIp
        for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
            // empty log slot
            if (mapElement.getValue() == null) continue;
            String log = mapElement.getValue().flatten();
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter("Log.txt"));
                writer.write(log);
                writer.close();
            } catch (IOException e) {
                System.out.println("write exception");
            }
        }
    }

    public boolean isConflict(ArrayList<Integer> flights) {
        // keep track of reserved flights in dict, mapping flight number to count
        HashMap<Integer, Integer> ReservedFlights = new HashMap<>();

        for (int i = 0; i < dictionary.size(); i++) {
            // reserved flights for each reservation provided in local dictionary
            ArrayList<Integer> curLocalReservedFlights = dictionary.get(i).getFlights();
            for (int j = 0; j < curLocalReservedFlights.size(); j++) {
                Integer curFlight = curLocalReservedFlights.get(j);
                // update current flight counts in dictionary
                Integer curCnt = ReservedFlights.get(curFlight); // current flight counts in dictionary
                ReservedFlights.put(curFlight, (curCnt == null) ? 1 : curCnt + 1);
            }
        }

        for (int i = 0; i < flights.size(); i++) {
            int flight = flights.get(i);
            // if there exists a flight that is fully booked in log, that is a conflict
            if (!ReservedFlights.isEmpty() && ReservedFlights.get(flight) != null) {
                if (ReservedFlights.get(flight) == 2) {
                    return true;
                }
            }
        }
        return false;
    }

    // -1: conflicted. 0: not added. 1: success
    public int insert (String[] orderInfo) {
        // get the maxSlot index of current log
        int maxSlot = 0;
        for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
            maxSlot = Math.max(maxSlot, mapElement.getKey());
        }
        for (int i = 0; i < maxSlot; i++) {
            // TODO: check key first then value next
            // there is a hole in local log
            if (this.log.get(i) == null) {
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
                    this.log.put(i, new Reservation(proposer.getMaxVal()));
                    this.store();
                    // update local dict
                    for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
                        if (mapElement.getValue() == null) continue;
                        Reservation curResv = mapElement.getValue();
                        if (this.dictionary.contains(curResv)) continue;
                        this.dictionary.add(curResv);
                    }
                }
                else {
                    // TODO: learn hole failed
                    return 0;
                }
            }
        }

        // 1. detect conflict
        String clientName = orderInfo[1];
        ArrayList<Integer> flights = new ArrayList<>();
        for (String s : orderInfo[2].split(",")) {
            flights.add(Integer.parseInt(s));
        }
        if (isConflict(flights)) return -1;

        Reservation newResv = new Reservation("insert", clientName, flights);
        String reservation = newResv.flatten();


        // 3. propose for the chosen slot
        this.proposer.setNext_log_slot(maxSlot);
        this.proposer.setReservation(reservation);
        int cnt = 3;
        while (cnt > 0) {
            int proposeRet = proposer.propose();
            // if successfully proposed
            if (proposeRet == 1) break;
            // if failed in the competing of the chosen slot, need to propose for the next slot
            else if(proposeRet == -1) this.proposer.setNext_log_slot(maxSlot + 1);
            cnt--;
        }
        // successfully proposed
        if (cnt > 0) {
            // update log entry
            this.log = this.proposer.learn(this.log);
            this.store();
            // update local dict
            for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
                if (mapElement.getValue() == null) continue;
                Reservation curResv = mapElement.getValue();
                if (this.dictionary.contains(curResv)) continue;
                this.dictionary.add(curResv);
            }
            return 1;
        }
        else {
            // not adding to the log
            // TODO: notify user
            return 0;
        }
    }

    // -1: deleted before. 0: not added. 1: success
    public int delete (String[] orderInfo) {
        // TODO: detect holes in log and update dictionary
        // get the maxSlot index of current log
        int maxSlot = 0;
        for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
            maxSlot = Math.max(maxSlot, mapElement.getKey());
        }
        for (int i = 0; i < maxSlot; i++) {
            // TODO: check key first then value next
            // there is a hole in local log
            if (this.log.get(i) == null) {
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
                    this.log.put(i, new Reservation(proposer.getMaxVal()));
                    this.store();
                    // update local dict
                    for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
                        if (mapElement.getValue() == null) continue;
                        Reservation curResv = mapElement.getValue();
                        if (this.dictionary.contains(curResv)) continue;
                        this.dictionary.add(curResv);
                    }
                }
                else {
                    // TODO: learn hole failed
                    return 0;
                }
            }
        }

        String clientName = orderInfo[1];
        ArrayList<Integer> flights = new ArrayList<>();
        for (String s : orderInfo[2].split(",")) {
            flights.add(Integer.parseInt(s));
        }

        Reservation newResv = new Reservation("delete", clientName, flights);
        String reservation = newResv.flatten();

        // 2. check whether it has been deleted before
        for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
            if (newResv.equals(mapElement.getValue())) {
                // cancel request failed
                return -1;
            }
        }

        // 3. propose for the chosen slot
        this.proposer.setNext_log_slot(maxSlot);
        this.proposer.setReservation(reservation);
        int cnt = 3;
        while (cnt > 0) {
            // if successfully proposed
            if (proposer.propose() == 1) break;
            cnt--;
        }
        // successfully proposed
        if (cnt > 0) {
            // update log entry
            this.log = this.proposer.learn(this.log);
            this.store();
            // update local dict
            ArrayList<Reservation> newDict = new ArrayList<>();
            for (Map.Entry<Integer, Reservation> mapElement: this.log.entrySet()) {
                if (mapElement.getValue() == null) continue;
                Reservation curResv = mapElement.getValue();
                newDict.add(curResv);
            }
            this.dictionary = newDict;
            return 1;
        }
        else {
            // not adding to the log
            return 0;
        }
    }
}
