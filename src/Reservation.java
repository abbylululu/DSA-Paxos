import java.lang.String;
import java.util.ArrayList;

public class Reservation {
    private String operation;
    private String clientName;
    private ArrayList<Integer> flights;

    // CONSTRUCTOR
    Reservation(String operation, String clientName, ArrayList<Integer> flights) {
        this.operation = operation;
        this.clientName = clientName;
        this.flights = flights;
    }
    // accVal format: operation clientName flights
    Reservation(String accVal) {
        String[] splitted = accVal.split(" ");
        this.operation = splitted[0];
        this.clientName = splitted[1];

        ArrayList<Integer> newFlights = new ArrayList<>();
        for (int i = 2; i < splitted.length; i++) {
            newFlights.add(Integer.parseInt(splitted[i]));
        }
        this.flights = newFlights;
    }

    // HELPER
    public String flatten() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.operation);
        sb.append(" ");
        sb.append(this.clientName);
        for (int i = 0; i < this.flights.size(); i++) {
            sb.append(" ");
            sb.append(flights.get(i));
        }
        return sb.toString();
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
}
