import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Host {
    public static String curSiteId;
    public static String curStartPort;
    public static String curEndPort;
    public static String curIp;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // get all sites' information from knownhost
        // store info into a hashmap, property -> info, arranged by index of each site
        ArrayList<HashMap<String, String>> sitesInfo = new ArrayList<>();
        int siteNum = 0;
        // read host name and port number from json
        try {
            JSONParser parser = new JSONParser();
            JSONObject data = (JSONObject) parser.parse(new FileReader("./knownhosts.json"));
            JSONObject hosts = (JSONObject) data.get("hosts");

            ArrayList<String> allSiteId = new ArrayList<>();

            // indice each siteId by siteId comparison
            hosts.keySet().forEach(siteId ->
            {
                allSiteId.add(siteId.toString());
            });
            Collections.sort(allSiteId);

            // initialze array storing all informations of all sites
            siteNum = allSiteId.size();
            for (int i = 0; i < siteNum; i++) {
                HashMap<String, String> tmp = new HashMap<>();
                sitesInfo.add(tmp);
            }

            hosts.keySet().forEach(siteId ->
            {
                JSONObject siteInfo = (JSONObject) hosts.get(siteId);
                String Id = siteId.toString();

                String udpStartPort = siteInfo.get("udp_start_port").toString();
                String udpEndPort = siteInfo.get("udp_end_port").toString();
                String ipAddr = (String) siteInfo.get("ip_address");

                Integer siteIndex = 0;
                for (int i = 0; i < allSiteId.size(); i++) {
                    if (allSiteId.get(i).equals(Id)) siteIndex = i;
                }

                HashMap<String, String> tmp = new HashMap<>();
                tmp.put("startPort", udpStartPort);
                tmp.put("endPort", udpEndPort);
                tmp.put("ip", ipAddr);
                tmp.put("siteId", siteId.toString());

                sitesInfo.set(siteIndex, tmp);
            });

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

//==================================================================================================
        // ---------test---------//
        String id = args[0];

        // find current site info
        curSiteId = "";
        curStartPort = "";
        curEndPort = "";
        curIp = "";

        InetAddress inetAddress = InetAddress.getLocalHost();
        curIp = inetAddress.getHostAddress();

        // FIXME: need to change back to ip
        int uid = 0; // site index as unique identifier
        for (int i = 0; i < sitesInfo.size(); i++) {
            if (sitesInfo.get(i).get("siteId").equals(id)) {
                HashMap<String, String> curMap = sitesInfo.get(i);
                curSiteId = curMap.get("siteId");
                curStartPort = curMap.get("startPort");
                curEndPort = curMap.get("endPort");
                uid = i;
            }
        }

//==================================================================================================
        // Blocking Queue
        BlockingQueue<String> proposerQueue = new ArrayBlockingQueue<String>(1024);
        BlockingQueue<String> learnerQueue = new ArrayBlockingQueue<String>(1024);
//==================================================================================================
        // Start port is for listening
        // End port is for sending
        // Create receive socket by start port number
        Integer receivePort = Integer.parseInt(curStartPort);
        // Create send socket by end port number
        Integer sendPort = Integer.parseInt(curEndPort);
        DatagramSocket receiveSocket = new DatagramSocket(receivePort);
        DatagramSocket sendSocket = new DatagramSocket(sendPort);

        new Acceptor(proposerQueue, learnerQueue, receiveSocket, sendSocket, sitesInfo).start();// child thread go here
        Proposer proposer = new Proposer(uid, sitesInfo, sendSocket, proposerQueue);
        new Learner(learnerQueue, proposer).start();
//==================================================================================================
        // TODO: store and recover log
        // Restore when site crashes
//        File logFile = new File("log.txt");
//        if (logFile.exists()) {
//            mySite.recover();
//        }

        // main thread keeps receiving msgs from user at this site
        while (true) {
            //System.out.println("[test]Please enter the command: ");
            Scanner in = new Scanner(System.in);
            String commandLine = in.nextLine();
            String[] input = commandLine.split("\\s+");

            if (input[0].equals("reserve")) {
                if (input.length != 3) continue;
                // process input
                Reservation newResv = processInput(input, curIp);
                // learn hole
                learnHole(proposer);
                // detect conflict
                assert newResv != null;
                if(isConflict(newResv.getFlights())) {
                    System.err.println("Conflict reservation for " + input[1] + ".");
                    continue;
                }
                // choose a slot to propose
                int logSlot = chooseSlot();
                boolean res;
                if (optimization(curIp, logSlot)) {// 1 phase
                    res = proposer.startOptimizedSynod(logSlot, newResv.flatten());
                } else {// 2 phases
                    res = proposer.startSynod(logSlot, newResv.flatten());
                }
                if (!res) {
                    System.out.println("Cannot schedule reservation for " + input[1] + ".");
                } else {
                    System.out.println("Reservation submitted for " + input[1] + ".");
                }


            } else if (input[0].equals("cancel")) {
                if (input.length != 2) continue;
                // learn hole
                learnHole(proposer);
                // check if previously deleted
                if(prevDel(commandLine)) {
                    System.err.println("previously deleted cancel for " + input[1] + ".");
                    continue;
                }
                // choose a slot to propose
                int logSlot = chooseSlot();
                boolean res;
                if (optimization(curIp, logSlot)) {// 1 phase
                    res = proposer.startOptimizedSynod(logSlot, commandLine);
                } else {// 2 phases
                    res = proposer.startSynod(logSlot, commandLine);
                }
                if (res) {
                    System.out.println("Reservation for " + input[1] + " cancelled.");
                } else {
                    System.out.println("Cannot cancel reservation for " + input[1] + ".");
                }

            } else if (input[0].equals("view")) {// Print dictionary here
                printDictionary();

            } else if (input[0].equals("log")) {// Print log here
                printLog();

            } else if (input[0].equals("quit")) {
                System.exit(0);

            } else {
                System.out.println("Oops, something is going wrong here!");
            }
        }
    }

    //==================================================================================================
    public static int chooseSlot() {
        if (Learner.log.isEmpty()) {
            return 0;
        }
        int maxSlot = -1;
        maxSlot = Learner.getMaxLogSlot();
        return maxSlot + 1;
    }


    // FIXME: learn hole failed
    public static void learnHole(Proposer proposer) {
        int slot = chooseSlot();
        for (int i = 0; i < slot; i++) {
            Reservation curLog = Learner.log.get(i);
            if (curLog == null) {
                proposer.startSynod(i, "");
            }
        }
    }


    public static boolean isConflict(ArrayList<Integer> flights) {
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
        return false;
    }


    public static Reservation processInput(String[] input, String proposerIp) {
        String clientName = input[1];
        // reserve
        if (input.length == 3) {
            String[] parsedFlights = input[2].split(",");
            ArrayList<Integer> newFlights = new ArrayList<>();
            for(int i = 0; i < parsedFlights.length; i++) {
                newFlights.add(Integer.parseInt(parsedFlights[i]));
            }
            return new Reservation("reserve", clientName, newFlights);
        }
        return null;
    }


    public static boolean prevDel(String newCancel) {
        for (Map.Entry<Integer, Reservation> mapElement: Learner.log.entrySet()) {
//            System.out.println("***equal?" + mapElement.getValue().trim().equals(newCancel));
            if (newCancel.equals(mapElement.getValue().flatten())) {
                return true;
            }
        }
        return false;
    }


    public static void printDictionary() {
        ArrayList<Reservation> newDict = Learner.dictionary;
        newDict.sort(new CustomComparator());
        for (int i = 0; i < newDict.size(); i++) {
            System.out.println(newDict.get(i).getClientName() + " " + newDict.get(i).getPrintFlight());
        }
    }


    public static void printLog() {
        for(Map.Entry<Integer,Reservation> entry : Learner.log.entrySet()) {
            Reservation value = entry.getValue();
            if (value.getOperation().equals("reserve")) {
                System.out.println(value.getOperation() + " "
                        + value.getClientName() + " " + value.getPrintFlight());
            } else {
                System.out.println(value.getOperation() + " " + value.getClientName());
            }
        }
    }

    public static boolean optimization(String curIp, int curLogSlot) {
        if (curLogSlot < 1) return false;
        int prevLogSlot = curLogSlot - 1;
        if (Acceptor.proposerIp.containsKey(prevLogSlot)) {
            String prevIp = Acceptor.proposerIp.get(prevLogSlot);
            if (prevIp.equals(curIp)) {
                return true;
            }
        }
        return false;
    }


}