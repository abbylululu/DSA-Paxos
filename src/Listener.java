import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;

/*
The implementation of UDP listener
Listen the msgs from a specific site
 */
public class Listener extends Thread {
    private DatagramSocket socket;
    private boolean running;
    private byte[] buffer = new byte[65535];
    ArrayList<HashMap<String, String>> sitesInfo;
    ReservationSys mySite;

    // CONSTRUCTOR
    public Listener(DatagramSocket updSocket, ArrayList<HashMap<String, String>> sitesInfo, ReservationSys mySite) {
        this.socket = updSocket;
        this.running = true;
        this.sitesInfo = sitesInfo;
        this.mySite = mySite;
    }

    // HELPERS
    public void run() {
        DatagramPacket packet = null;
        while (this.running) {
            // Receive from other server
            packet = new DatagramPacket(this.buffer, this.buffer.length);
            try {
                socket.receive(packet);// blocks until a msg arrives
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Processing information
            String senderIp = packet.getAddress().getHostAddress();
            String senderId = null;
            for (int i = 0; i < this.sitesInfo.size(); i++) {
                if (this.sitesInfo.get(i).get("ip").equals(senderIp) &&
                        !this.sitesInfo.get(i).get("siteId").equals(this.mySite.getSiteId())) {
                    senderId = this.sitesInfo.get(i).get("siteId");
//                    System.out.println("[test] Got something from site " + senderId);
                    break;
                }
            }

            // Update the current site based on the received information
            try {
                mySite.update((CommunicateInfo) deserialize(packet.getData()), senderId);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            // TODO: send received confirmation?

            this.buffer = new byte[65535];//reset
        }
        socket.close();
    }

    // Deserialize the byte array and reconstruct the object
    public static Object deserialize(byte[] buffer) throws IOException, ClassNotFoundException{
        ByteArrayInputStream byteStream = new ByteArrayInputStream(buffer);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);
        return objStream.readObject();
    }
}