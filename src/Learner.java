import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class Learner extends Thread{
    private BlockingQueue learnerQueue = null;
    static ArrayList<Reservation> dictionary; // local reservation data structure
    static HashMap<Integer, String> log; // array of reservation string(ops clientName 1 2 3), in stable storage

    public Learner(BlockingQueue learnerQueue) {
        this.learnerQueue = learnerQueue;
    }
}
