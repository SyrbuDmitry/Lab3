package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public Tuple2<String,String> flightKey;
    public String delay;
    
}
