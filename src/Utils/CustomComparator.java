package Utils;

import Messages.Reservation;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public class CustomComparator implements Comparator<Reservation> {
    @Override
    public int compare(@NotNull Reservation o1, @NotNull Reservation o2) {
        return o1.getClientName().compareTo(o2.getClientName());
    }
}
