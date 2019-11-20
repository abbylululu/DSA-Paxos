import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Host {
    public static void main(String[] args) throws IOException {
        // get all sites' information from knownhost
        // store info into a hashmap, property -> info, arranged by index of each site
        ArrayList<HashMap<String, String>> sitesInfo = new ArrayList<>();
        Integer siteNum = 0;
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

        // ---------test---------//
        String id = args[0];

        // find current site info
        String curSiteId = "";
        String curStartPort = "";
        String curEndPort = "";
        String curIp = "";

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
        // FIXME: separate directory and project structure
        // Restore when site crashes
        File timeFile = new File("timeTable.txt");
        File dictFile = new File("dictionary.txt");
        File logFile = new File("log.txt");
        if (timeFile.exists() && dictFile.exists() && logFile.exists()) {
            mySite.recover(siteNum);
        }
//==================================================================================================
        // Blocking Queue
        BlockingQueue queue = new ArrayBlockingQueue(1024);
//==================================================================================================
        //TODO: UDP Transportation
        // Start port is for listening
        // End port is for sending
        // Create receive socket by start port number
        Integer receivePort = Integer.parseInt(curStartPort);
        DatagramSocket receiveSocket = new DatagramSocket(receivePort);
        // Create send socket by end port number
        DatagramSocket sendSocket = new DatagramSocket(Integer.parseInt(curEndPort));

        //TODO: Start from here
        // Construct current site(work as proposer, acceptor, learner simultaneously)
        ReservationSys mySite = new ReservationSys(sitesInfo, uid, sendSocket);

        new Acceptor(queue, receiveSocket, sendSocket, sitesInfo, mySite).start();// child thread go here
        
//==================================================================================================
        // TODO: UI
        // main thread keeps receiving msgs from user at this site
        while (true) {
            System.out.println("[test]Please enter the command: ");
            Scanner in = new Scanner(System.in);
            String commandLine = in.nextLine();
            String[] input = commandLine.split("\\s+");

            if (input[0].equals("reserve")) {// insert into my site, update timetable, log and dictionary
                // TODO: how to handle conflicted reserve?
                if (mySite.insert(input) == 0) {
                    // TODO: notify user, time out and not adding
                }

            } else if (input[0].equals("cancel")) {// delete from my site's dictionary, update log and timetable
                if (mySite.delete(input) == -1) {
                    System.out.println("Cancel Error");
                }
                else if (mySite.delete(input) == 0) {
                    // TODO: notify user, time out and not adding
                }

            } else if (input[0].equals("view")) {// Print dictionary here
//                mySite.printDictionary();

            } else if (input[0].equals("log")) {// Print log here
//                mySite.printLog();

            } else if (input[0].equals("quit")) {
                System.exit(0);

            } else {
                System.out.println("Oops, something is going wrong here!");
            }
        }
    }

    //==================================================================================================
    //TODO: Helper function
    // Serialize the CommunicateInfo to byte array
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        return bos.toByteArray();
    }

    // Only send the log that is identified as necessary
    public static void sendMsgToOthers(ReservationSys mySite, DatagramSocket sendSocket,
                                       ArrayList<HashMap<String, String>> sitesInfo,
                                       ArrayList<String> recipients) throws IOException {
        // build one message for all other sites
        byte[] sendArray = serialize(buildMsg(mySite, recipients, sitesInfo));
        UDPSend(recipients, sitesInfo, sendArray, sendSocket);
    }

    public static void UDPSend(ArrayList<String> recipients, ArrayList<HashMap<String, String>> sitesInfo, byte[] sendArray, DatagramSocket sendSocket) throws IOException {
        for (int i = 0; i < recipients.size(); i++) {
            String ipAddress = null;
            String receivePort = null;
            for (int j = 0; j < sitesInfo.size(); j++) {
                if (sitesInfo.get(j).get("siteId").equals(recipients.get(i))) {
                    ipAddress = sitesInfo.get(j).get("ip");
                    receivePort = sitesInfo.get(j).get("startPort");
                    break;
                }
            }
            InetAddress targetIP = InetAddress.getByName(ipAddress);
            DatagramPacket sendPacket = new DatagramPacket(sendArray, sendArray.length, targetIP, Integer.parseInt(receivePort));
            sendSocket.send(sendPacket);
        }
    }
}