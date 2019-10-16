package mapreduce.apache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ApacheReduce {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc =new JavaSparkContext(conf);
        
    }
}
