package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public boolean isCanceled;
    FlightLine(String delay, String canceled){
        double c = Double.parseDouble(canceled);
        if(c == 0) {
            this.delay = -1;
            isCanceled = true;
        }
        else {
            isCanceled = false;
            this.delay = Double.parseDouble(delay);
        }
    }
}
