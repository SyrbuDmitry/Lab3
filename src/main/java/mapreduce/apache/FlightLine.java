package mapreduce.apache;

import scala.Tuple2;

import java.io.Serializable;

public class FlightLine implements Serializable {
    public double delay;
    public double maxDelay = 0;
    public boolean isCanceled;
    public int counter,lateCounter,canceledCounter;
    FlightLine(){}

    FlightLine(double delay, int counter, int lateCounter, int canceledCounter){
        this.delay=delay;
        this.counter=counter;
        this.lateCounter=lateCounter;
        this.canceledCounter=canceledCounter;
    }
    FlightLine(String d, String canceled){
        counter=1;
        if (canceled.equals("0.00")){
            if (d.equals(""))
                delay=0;
            else {
                delay = Double.parseDouble(d);
                if(delay!=0)
                    lateCounter = 1;
                isCanceled = false;
            }
        }else{
            isCanceled=true;
            canceledCounter= 1;
            maxDelay=delay=-1;
        }
    }
}
