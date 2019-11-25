import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Send extends Thread{
    private String targetIp;
    private int receivePort; // allocated start port
    DatagramSocket sendSocket;
    String message;

    public Send(String targetIp, int receivePort, DatagramSocket sendSocket, String message) {
        this.targetIp = targetIp;
        this.receivePort = receivePort;
        this.sendSocket = sendSocket;
        this.message = message;
    }

    // Serialize the CommunicateInfo to byte array
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        return bos.toByteArray();
    }

    public void run() {
        byte[] sendArray = new byte[0];
        try {
            sendArray = serialize(this.message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        DatagramPacket sendPacket = null;
        try {
            sendPacket = new DatagramPacket(sendArray, sendArray.length,
                    InetAddress.getByName(this.targetIp), this.receivePort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            assert sendPacket != null;
            this.sendSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
