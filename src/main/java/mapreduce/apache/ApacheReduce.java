package mapreduce.apache;

import com.sun.corba.se.spi.orb.ParserData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ApacheReduce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flights = sc.textFile("/user/dmitrijsyrbu/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airports = sc.textFile("/user/dmitrijsyrbu/L_AIRPORT_ID.csv");

        JavaRDD<String[]> airportsSplited = airports
                .filter(x -> !x.startsWith("Code"))
                .map(s -> Arrays.stream(s.split(",(?=\")"))
                        .toArray(String[]::new)
                );

        JavaRDD<String[]> flightsSplited = flights
                .filter(x -> !x.startsWith("\"YEAR\""))
                .map(s -> Arrays.stream(s.split(","))
                        .toArray(String[]::new)
                );

        JavaPairRDD<String,String> a = airportsSplited.mapToPair(
                s->new Tuple2<>(s[0].replaceAll("\"",""),s[1])
        );
        Map<String,String> airportMap = a.collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportMap);

        JavaPairRDD<Tuple2<String, String>, FlightLine> f = flightsSplited.mapToPair(
                s -> new Tuple2<>(new Tuple2<>(s[11], s[14]), new FlightLine(s[18], s[19])));

        JavaPairRDD<Tuple2<String, String>, FlightLine> res = f.reduceByKey(new Function2<FlightLine, FlightLine, FlightLine>() {
            @Override
            public FlightLine call(FlightLine a, FlightLine b) {
                double maxDelay = 0;
                if(a.delay>b.delay)
                    maxDelay = a.delay;
                else maxDelay=b.delay;
                int c = a.counter+b.counter;
                int lc = a.lateCounter+b.lateCounter;
                int cc = a.canceledCounter+b.canceledCounter;
                return new FlightLine(maxDelay,c,lc,cc);
            }
        });
        JavaPairRDD<Tuple2<String, String>,List<String>> newRes = res.mapToPair(
          s->new Tuple2<>(s._1,Arrays.asList(String.valueOf(s._2.delay),
                  String.format("%.2f %%",((double)s._2.lateCounter/s._2.counter)*100),
                  String.format("%.2f %%",((double)s._2.canceledCounter/s._2.counter)*100))
        ));

        JavaRDD<List<String>> ress = newRes.map(
                s->Arrays.asList(s._2,airportsBroadcasted.value().get(s._1))
        );
        a.saveAsTextFile("/user/dmitrijsyrbu/sparkoutput");
    }

}
