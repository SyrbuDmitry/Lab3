package mapreduce.apache;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataTransformer {
    public static Function2<FlightLine, FlightLine, FlightLine> reduceFunc = new Function2<FlightLine, FlightLine, FlightLine>(){
        @Override
        public FlightLine call(FlightLine a, FlightLine b) {
            double maxDelay;
            if (a.delay > b.delay)
                maxDelay = a.delay;
            else maxDelay = b.delay;
            int c = a.counter + b.counter;
            int lc = a.lateCounter + b.lateCounter;
            int cc = a.canceledCounter + b.canceledCounter;
            return new FlightLine(maxDelay, c, lc, cc);
        }
    };
    public static JavaRDD<String[]> splitAirports(JavaRDD<String> airports){
        return airports
                .filter(x -> !x.startsWith("Code"))
                .map(s -> Arrays.stream(s.split(",(?=\")"))
                        .toArray(String[]::new));
    }

    public static JavaRDD<String[]> splitFlights(JavaRDD<String> flights){
        return flights
                .filter(x -> !x.startsWith("\"YEAR\""))
                .map(s -> Arrays.stream(s.split(","))
                        .toArray(String[]::new)
                );
    }

    public static JavaPairRDD<Tuple2<String, String>, List<String>> resultMapPair(JavaPairRDD<Tuple2<String, String>, FlightLine> res){
        return res.mapToPair(
                s -> new Tuple2<>(s._1, Arrays.asList(String.valueOf(s._2.delay),
                        String.format("%.2f %%", ((double) s._2.lateCounter / s._2.counter) * 100),
                        String.format("%.2f %%", ((double) s._2.canceledCounter / s._2.counter) * 100))
                ));
    }
    public static JavaRDD<List<String>> outputTransform(JavaPairRDD<Tuple2<String, String>, List<String>> newRes,Broadcast<Map<String, String>> airportsBroadcasted){
        return  newRes.map(
                s -> Arrays.asList(airportsBroadcasted.value().get(s._1._1), airportsBroadcasted.value().get(s._1._2), s._2.toString())
        );
    }
    public static JavaPairRDD<String, String> airportsPair(JavaRDD<String[]> airportsSplited){
        return airportsSplited.mapToPair(
                s -> new Tuple2<>(s[0].replaceAll("\"", ""), s[1])
        );
    }

    public static JavaPairRDD<Tuple2<String, String>, FlightLine> flightsPair(JavaRDD<String[]> flightsSplited){
        return flightsSplited.mapToPair(
                s -> new Tuple2<>(new Tuple2<>(s[11], s[14]), new FlightLine(s[18], s[19])));

    }

}
