import java.io.Serializable;
import java.lang.String;
import java.util.ArrayList;
import java.util.Comparator;

public class Reservation implements Serializable {
    private String operation;
    private String clientName;
    private ArrayList<Integer> flights;
    private String proposerIp;
    private boolean isCheckPoint;
    private String printString;

    // CONSTRUCTOR
    Reservation(String operation, String clientName, String proposerIp, ArrayList<Integer> flights) {
        this.operation = operation;
        this.clientName = clientName;
        this.flights = flights;
        this.proposerIp = proposerIp;
        this.isCheckPoint = false;
        this.printString = null;
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

    public String getPrintFlight() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.flights.size(); i++) {
            sb.append(flights.get(i));
            if (i < this.flights.size() - 1) sb.append(",");
        }
        return sb.toString();
    }

    // GETTERS and SETTERS

    public boolean isCheckPoint() {
        return this.isCheckPoint;
    }

    public String getPrintString() {
        return this.printString;
    }

    public String getProposerIp() {
        return this.proposerIp;
    }

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

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public void setProposerIp(String proposerIp) {
        this.proposerIp = proposerIp;
    }

    public void setCheckPoint(boolean checkPoint) {
        this.isCheckPoint = checkPoint;
    }

    public void setPrintString(String printString) {
        this.printString = printString;
    }
}


class CustomComparator implements Comparator<Reservation> {
    @Override
    public int compare(Reservation o1, Reservation o2) {
        return o1.getClientName().compareTo(o2.getClientName());
    }
}
