package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public boolean isCanceled;
    public FlightLine(String delay, String canceled){

        if(canceled.equals("1.00")) {
            this.delay = -1;
            isCanceled = true;
        } else {
            isCanceled = false;
            this.delay = Double.parseDouble(delay);
        }
    }
}
