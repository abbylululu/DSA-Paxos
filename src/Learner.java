import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class Learner {
    private ArrayList<Reservation> reservations;
    private File logFile;

    public Learner() {
        this.reservations = new ArrayList<>();
        this.logFile = new File("Log.txt");
    }

    public void handleMsg(String commitMsg) {
        //commit("commit accVal logSlot senderIP")
        String[] parameters = commitMsg.split(" ");
        String log = parameters[2] + " " + parameters[1] + " " + parameters[3];
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("Log.txt"));
            writer.write(log);
            writer.close();
        } catch (IOException e) {
            System.out.println("write exception");
        }
    }
}
