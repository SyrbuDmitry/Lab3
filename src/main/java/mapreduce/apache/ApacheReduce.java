package mapreduce.apache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class ApacheReduce {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flights = sc.textFile("/user/dmitrijsyrbu/664600583_T_ONTIME_sample.csv");
        JavaRDD<String[]> flightsSplited = flights
                .filter(x->!x.startsWith("\"YEAR\""))
                .map(s->Arrays.stream(s.split(","))
                .toArray(String[]::new)
        );

//        JavaPairRDD<Tuple2<String,String>,FlightLine> f = flightsSplited.mapToPair(
//                s->new Tuple2<>(new Tuple2<>(s[11],s[14]),new FlightLine(s[18],s[19])));
//
//        JavaPairRDD<Tuple2<String,String>,FlightLine> res = f.reduceByKey(new Function2<FlightLine,FlightLine,FlightLine>(){
//            @Override
//            public FlightLine call(FlightLine a,FlightLine b){
//                if(a.delay<b.delay)
//                    a.delay = b.delay;
//                return a;
//            }
//        });

        flightsSplited.saveAsTextFile("/user/dmitrijsyrbu/sparkoutput");
    }
}
