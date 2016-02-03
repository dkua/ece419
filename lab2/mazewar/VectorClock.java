import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by dkua on 2016-02-02.
 */
public class VectorClock implements Comparable<VectorClock>, Serializable {

    public static final int LESS = -1;
    public static final int EQUAL = 0;
    public static final int GREATER = 1;
    private HashMap<String, Integer> clocks = new HashMap<String, Integer>();

    public VectorClock(Player[] players) {
        for (Player player : players) {
            clocks.put(player.name, 0);
        }
    }

    public void increment(Player player) {
        Integer value = clocks.get(player.name);
        clocks.put(player.name, value + 1);
    }

    public void increment(String name) {
        Integer value = clocks.get(name);
        clocks.put(name, value + 1);
    }

    public Integer get(String name) {
        return clocks.get(name);
    }

    public boolean contains(String name) {
        return clocks.containsKey(name);
    }

    public void replace(VectorClock vc) {
        for (String player : clocks.keySet()) {
            clocks.put(player, vc.get(player));
        }
    }

    public String toString() {
        return clocks.toString();
    }

    public int compareTo(VectorClock vc) {
        HashSet<Integer> status = new HashSet<Integer>();

        for (String player : clocks.keySet()) {
            if (vc.contains(player)) {
                if (vc.get(player) < clocks.get(player)) {
                    status.add(LESS);
                } else if (vc.get(player) > clocks.get(player)) {
                    status.add(GREATER);
                } else {
                    status.add(EQUAL);
                }
            } else {
                throw new VectorClockConflict();
            }
        }

        if (status.contains(LESS) && status.contains(GREATER)) {
            throw new VectorClockConflict();
        } else {
            Integer result = 0;
            for (Integer i : status) {
                result += i;
            }
            return result;
        }
    }
}
