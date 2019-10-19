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

public class ApacheApp {

    private static final String flightPath = "/user/dmitrijsyrbu/664600583_T_ONTIME_sample.csv";
    private static final String airportsPath = "/user/dmitrijsyrbu/L_AIRPORT_ID.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flights = sc.textFile(flightPath);
        JavaRDD<String> airports = sc.textFile(airportsPath);

        JavaRDD<String[]> airportsSplited = DataTransformer.splitAirports(airports);

        JavaRDD<String[]> flightsSplited = DataTransformer.splitFlights(flights);

        JavaPairRDD<String, String> airPairs = DataTransformer.airportsPair(airportsSplited);

        JavaPairRDD<Tuple2<String, String>, FlightLine> flightsTuple = DataTransformer.flightsPair(flightsSplited);

        Map<String, String> airportMap = airPairs.collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportMap);

        JavaPairRDD<Tuple2<String, String>, FlightLine> dataReduced = flightsTuple.reduceByKey(DataTransformer.reduceFunc);

        JavaPairRDD<Tuple2<String, String>, List<String>> dataResult = DataTransformer.resultMapPair(dataReduced);

        JavaRDD<List<String>> resOut = DataTransformer.outputTransform(dataResult, airportsBroadcasted);

        resOut.saveAsTextFile("/user/dmitrijsyrbu/sparkoutput");
    }

}
