import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PlayingWIthRDD {
    public static void main(String[] args) {
        String logFile = "resources/spark-2.4.4-bin-hadoop2.7/README.md";
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(logFile);
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("the files contains "+totalLength+" lines");
    }
}
