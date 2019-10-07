import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Function2;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.*;
import scala.collection.generic.CanBuildFrom;
import scala.collection.mutable.Builder;

import java.util.Arrays;
import java.util.List;

public class ReadParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        String parquetFile = "/Users/dellavalle/work/Code/tmp-parquet/store_sales";

        Dataset<Row> tempDF = spark.read().option("inferSchema",true).parquet(parquetFile);

        tempDF.printSchema();
        tempDF.show(10);
        List<String> cols = Arrays.asList("ss_item_sk","ss_sales_price");

        Dataset<Row> orderedDF = tempDF
                .orderBy(col("ss_sales_price").desc_nulls_last())
                .select("ss_item_sk", "ss_net_profit", "ss_wholesale_cost")
                .withColumn("profit", expr("ss_wholesale_cost-ss_net_profit"));
        orderedDF.show(10);
    }
}
