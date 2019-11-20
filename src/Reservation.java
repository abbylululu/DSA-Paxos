import java.io.Serializable;
import java.util.ArrayList;

public class Reservation implements Serializable {
    private static final long serialVersionUID = 420752690109126838L;
    private String clientName;
    private ArrayList<Integer> flights;
    private String status;

    // TODO: change form into: operation clientName FlightNumber
    // CONSTRUCTOR
    Reservation(String clientName, String status, ArrayList<Integer> flights) {
        this.clientName = clientName;
        this.status = status;
        this.flights = flights;
    }

    // GETTERS and SETTERS
    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ArrayList<Integer> getFlights() {
        return flights;
    }

    public void setFlights(ArrayList<Integer> flights) {
        this.flights = flights;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }


    // HELPERS
    /**
     * flatten object into the format of: "clientName flightsNumber status"
     */
    public String flatten() {
        StringBuilder builder = new StringBuilder();
        // clientName
        builder.append(this.clientName);
        builder.append(" ");
        // flights
        for (int i = 0; i < this.flights.size(); i++) {
            builder.append(this.flights.get(i));
            if (i < this.flights.size() - 1) builder.append(",");
        }
        builder.append(" ");
        // status
        builder.append(this.status);
        builder.append(" ");

        return builder.toString();
    }

    public String printFlight() {
        if (this.flights.isEmpty()) return "";
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < this.flights.size(); i++) {
            if (i != this.flights.size() - 1) res.append(this.flights.get(i)).append(",");
            else res.append(this.flights.get(i));
        }
        return res.toString();
    }
}
