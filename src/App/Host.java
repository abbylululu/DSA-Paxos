package App;

import Utils.CustomComparator;
import Messages.Record;
import Messages.Reservation;
import Roles.Acceptor;
import Roles.Learner;
import Roles.Proposer;
import Utils.SendUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    public static String curIp;
    private static ArrayList<HashMap<String, String>> sitesInfo = new ArrayList<>();
    public static HashMap<String, String> lastSeen = new HashMap<>();

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // get all sites' information from knownhost
        // store info into a hashmap, property -> info, arranged by index of each site
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
            int siteNum = allSiteId.size();
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

                int siteIndex = 0;
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

        String id = args[0];

        // find current site info
        curSiteId = "";
        String curStartPort = "";
        String curEndPort = "";
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

        // Blocking Queue
        BlockingQueue<String> proposerQueue = new ArrayBlockingQueue<>(1024);
        BlockingQueue<String> learnerQueue = new ArrayBlockingQueue<>(1024);

        // Start port is for listening
        // End port is for sending
        // Create receive socket by start port number
        int receivePort = Integer.parseInt(curStartPort);
        // Create send socket by end port number
        int sendPort = Integer.parseInt(curEndPort);
        DatagramSocket receiveSocket = new DatagramSocket(receivePort);
        DatagramSocket sendSocket = new DatagramSocket(sendPort);

        new Acceptor(proposerQueue, learnerQueue, receiveSocket, sendSocket, sitesInfo).start();// child thread go here
        Proposer proposer = new Proposer(uid, sitesInfo, sendSocket, proposerQueue);
        new Learner(learnerQueue, proposer, sitesInfo, sendSocket).start();

        // Restore last seen map
        File logFile = new File(Host.curSiteId + "lastSeen.txt");
        if (logFile.exists()) {
            Host.recoverLastSeen();
        }

        // main thread keeps receiving msgs from user at this site
        while (true) {
            System.out.println("Please enter the command: ");
            Scanner in = new Scanner(System.in);
            String commandLine = in.nextLine();
            String[] input = commandLine.split("\\s+");

            switch (input[0]) {
                case "reserve": {
                    if (input.length != 3) continue;

                    lastSeen.put(input[1], curIp);
                    // store lastSeen
                    storeLastSeen();
                    // send to all sites
                    sendLastSeen(sendSocket);

                    // process input
                    Reservation newResv = processInput(input, curIp);
                    // learn hole
                    learnHole(proposer);
                    // detect conflict
                    assert newResv != null;
                    if (isConflict(newResv.getFlights()) || isConflictName(newResv.getClientName())) {
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
                        System.out.println("Messages.Reservation submitted for " + input[1] + ".");
                    }


                    break;
                }
                case "cancel": {
                    if (input.length != 2) continue;

                    lastSeen.put(input[1], curIp);
                    // store lastSeen
                    storeLastSeen();
                    // send to all sites
                    sendLastSeen(sendSocket);

                    // learn hole
                    learnHole(proposer);
                    // check if previously deleted
                    if (prevDel(commandLine)) {
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
                        System.out.println("Messages.Reservation for " + input[1] + " cancelled.");
                    } else {
                        System.out.println("Cannot cancel reservation for " + input[1] + ".");
                    }

                    break;
                }
                case "view": // Print dictionary here
                    printDictionary();

                    break;
                case "log": // Print log here
                    printLog();

                    break;
                case "quit":
                    System.exit(0);

                default:
                    System.out.println("Oops, something is going wrong here!");
                    break;
            }
        }
    }

    private static int chooseSlot() {
        if (Learner.log.isEmpty()) {
            return 0;
        }
        int maxSlot;
        maxSlot = Learner.getMaxLogSlot();
        return maxSlot + 1;
    }

    public static void learnHole(Proposer proposer) throws IOException {
        int slot = chooseSlot();
        for (int i = 0; i < slot; i++) {
            Reservation curLog = Learner.log.get(i);
            if (curLog == null) {
                proposer.startSynod(i, "");
            }
        }
    }

    public static void learnBackHole(Proposer proposer, Integer maxSlot) throws IOException {
        int slot = chooseSlot();
        int curMax = Learner.getMaxLogSlot();
        if (curMax == maxSlot) return;// no need to learn
        int index = maxSlot > slot ? maxSlot : slot;
        for (int i = 0; i <= index; i++) {
            if (!Learner.log.containsKey(i) || Learner.log.get(i) == null) {
                proposer.startSynod(i, "");
            }
        }
    }


    private static boolean isConflict(ArrayList<Integer> flights) {
        // keep track of reserved flights in dict, mapping flight number to count
        HashMap<Integer, Integer> ReservedFlights = new HashMap<>();
        if (Learner.dictionary.isEmpty()) return false;
        for (int i = 0; i < Learner.dictionary.size(); i++) {
            ArrayList<Integer> curLocalReservedFlights = Learner.dictionary.get(i).getFlights();
            for (Integer curFlight : curLocalReservedFlights) {
                if (ReservedFlights.containsKey(curFlight)) {
                    Integer curCnt = ReservedFlights.get(curFlight);
                    ReservedFlights.put(curFlight, curCnt + 1);
                } else {
                    ReservedFlights.put(curFlight, 1);
                }
            }
        }

        for (Integer flight : flights) {
            if (ReservedFlights.containsKey(flight) && ReservedFlights.get(flight) == 2) {
                return true;
            }
        }
        return false;
    }

    private static boolean isConflictName(String clientName) {
        if (Learner.dictionary.isEmpty()) return false;
        for (int i = 0; i < Learner.dictionary.size(); i++) {
            if (Learner.dictionary.get(i).getClientName().equals(clientName)) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private static Reservation processInput(@NotNull String[] input, String proposerIp) {
        String clientName = input[1];
        // reserve
        if (input.length == 3) {
            String[] parsedFlights = input[2].split(",");
            ArrayList<Integer> newFlights = new ArrayList<>();
            for (String parsedFlight : parsedFlights) {
                newFlights.add(Integer.parseInt(parsedFlight));
            }
            return new Reservation("reserve", clientName, newFlights);
        }
        return null;
    }


    private static boolean prevDel(String newCancel) {
        for (Map.Entry<Integer, Reservation> mapElement : Learner.log.entrySet()) {
            if (newCancel.equals(mapElement.getValue().flatten())) {
                return true;
            }
        }
        return false;
    }


    private static void printDictionary() {
        ArrayList<Reservation> newDict = Learner.dictionary;
        newDict.sort(new CustomComparator());
        for (Reservation reservation : newDict) {
            System.out.println(reservation.getClientName() + " " + reservation.getPrintFlight());
        }
    }


    private static void printLog() {
        for (Map.Entry<Integer, Reservation> entry : Learner.log.entrySet()) {
            Reservation value = entry.getValue();
            if (value.getOperation().equals("reserve")) {
                System.out.println(value.getOperation() + " "
                        + value.getClientName() + " " + value.getPrintFlight());
            } else {
                System.out.println(value.getOperation() + " " + value.getClientName());
            }
        }
    }

    private static boolean optimization(String curIp, int curLogSlot) {
        if (curLogSlot < 1) return false;
        int prevLogSlot = curLogSlot - 1;
        String prevClient = Learner.log.get(prevLogSlot).getClientName();
        return lastSeen.get(prevClient).equals(curIp);
    }

    public static void storeLastSeen() throws IOException {
        Record newR = new Record(null, null, null, null);
        newR.setLastSeen(Host.lastSeen);
        byte[] output = SendUtils.serialize(newR);
        File file = new File(Host.curSiteId + "lastSeen.txt");
        FileOutputStream fos;
        fos = new FileOutputStream(file);
        fos.write(output);
        fos.close();
    }

    private static void recoverLastSeen() throws IOException, ClassNotFoundException {
        Record recoverLastSeen =
                (Record) Acceptor.deserialize(Acceptor.readFromFile(Host.curSiteId + "lastSeen.txt"));
        Host.lastSeen = recoverLastSeen.getLastSeen();
    }

    @NotNull
    private static String lastSeenFlatten() {
        StringBuilder ret = new StringBuilder("lastSeen ");
        for (Map.Entry<String, String> mapElement : Host.lastSeen.entrySet()) {
            ret.append(mapElement.getKey());
            ret.append(",");
            ret.append(mapElement.getValue());
            ret.append(" ");
        }
        return ret.toString();
    }

    public static void sendLastSeen(DatagramSocket sendSocket) throws IOException {
        byte[] sendArray;
        sendArray = SendUtils.serialize(Host.lastSeenFlatten());
        DatagramPacket sendPacket;

        for (int i = 0; i < Host.sitesInfo.size(); i++) {
            sendPacket = new DatagramPacket(sendArray, sendArray.length,
                    InetAddress.getByName(Host.sitesInfo.get(i).get("ip")),
                    Integer.parseInt(Host.sitesInfo.get(i).get("startPort")));
            sendSocket.send(sendPacket);
        }
    }
}