package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public boolean isCanceled;
    FlightLine(String delay, String canceled){
        this.delay=Double.parseDouble(delay);
        
    }
}
