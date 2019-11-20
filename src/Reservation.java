import java.io.Serializable;
import java.util.ArrayList;

public class Reservation {
    private String operation;
    private String clientName;
    private ArrayList<Integer> flights;
    private String flattenString;

    // CONSTRUCTOR
    Reservation(String operation, String clientName, String flights) {
        this.operation = operation;
        this.clientName = clientName;
        this.flights = new ArrayList<>();
        String[] fNumber = flights.split(",");
        for (int i = 0; i < fNumber.length; i++) {
            this.flights.add(Integer.parseInt(fNumber[i]));
        }
        this.flattenString = operation  + " " + clientName + " " + flights;
    }

    // GETTERS and SETTERS
    public String getOperation() {
        return this.operation;
    }

    public String getClientName() {
        return this.clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ArrayList<Integer> getFlights() {
        return this.flights;
    }

    public void setFlights(ArrayList<Integer> flights) {
        this.flights = flights;
    }

    public String flatten() {
        return this.flattenString;
    }
}
