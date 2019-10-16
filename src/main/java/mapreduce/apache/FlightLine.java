package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public String delay;
    public boolean isCanceled;
}
