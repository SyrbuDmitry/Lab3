package mapreduce.apache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ApacheReduce {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flights = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<Tuple2<String,String>,FlightLine> = flights.mapToPair(
                s.split->)

    }
}
