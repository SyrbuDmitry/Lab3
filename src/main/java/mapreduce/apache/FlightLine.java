package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public boolean isCanceled;
    public int counter;
    public FlightLine(String d, String canceled){
        if (canceled.equals("0.00") && !d.equals("")){
            delay = Double.parseDouble(d);
            isCanceled = false;
        }else{
            isCanceled=true;
            delay=-1;
        }
    }
}
