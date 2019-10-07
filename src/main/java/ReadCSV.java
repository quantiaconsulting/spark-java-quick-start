import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class ReadCSV {
    public static void main(String[] args) {
        String csvFile = "/Users/dellavalle/work/Code/tmp-csv/store_sales.dat"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<Row> tempDF = spark.read().option("sep", "|").csv(csvFile);
        tempDF.cache();
        tempDF.printSchema();
        System.out.println(tempDF.count());
        spark.stop();
    }
}
