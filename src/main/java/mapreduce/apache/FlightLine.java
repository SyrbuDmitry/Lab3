package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public boolean isCanceled;
    public FlightLine(String delay, String canceled){
        double c = Double.parseDouble(canceled);
        if(c == 1) {
            this.delay = -1;
            isCanceled = true;
        } else if(c==0) {
            isCanceled = false;
            this.delay = Double.parseDouble(delay);
        }
    }
}
