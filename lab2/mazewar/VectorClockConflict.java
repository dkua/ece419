/**
 * Created by dkua on 2016-02-02.
 */
public class VectorClockConflict extends RuntimeException {
    public VectorClockConflict() {
    }

    ;

    public VectorClockConflict(String message) {
        super(message);
    }

    public VectorClockConflict(String message, Throwable throwable) {
        super(message, throwable);
    }
}
