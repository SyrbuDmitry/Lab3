package mapreduce.apache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ApacheReduce {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flights = sc.textFile("/Users/dmitrijsyrbu/Lab3/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airports = sc.textFile("/Users/dmitrijsyrbu/Lab3/L_AIRPORT_ID.csv");
    }
}
