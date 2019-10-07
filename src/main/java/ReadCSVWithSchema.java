import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class ReadCSVWithSchema {
    public static void main(String[] args) throws AnalysisException {

        String csvFile = "/Users/dellavalle/work/Code/tmp-csv/store_sales.dat";
        String parquetFile = "/Users/dellavalle/work/Code/tmp-parquet/store_sales";
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        List<StructField> fields = new ArrayList<>();

        StructField field = DataTypes.createStructField("ss_sold_date_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_sold_time_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_item_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_customer_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_cdemo_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_hdemo_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_addr_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_store_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_promo_sk", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_ticket_number", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_quantity", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_wholesale_cost", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_list_price", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_sales_price", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_ext_discount_amt", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_ext_sales_price", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_ext_wholesale_cost", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_ext_tax", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_coupon_amt", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_net_paid", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_net_paid_inc_tax", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_net_profit", DataTypes.DoubleType, true);
        fields.add(field);

        field = DataTypes.createStructField("ss_last", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> tempDF = spark.read().option("sep", "|").schema(schema).csv(csvFile);
        tempDF.cache();
        tempDF.printSchema();
        System.out.println(tempDF.count());
        tempDF.createTempView("store_sales");
        Dataset<Row> res = spark.sql("SELECT * FROM store_sales LIMIT 10");
        res.show();
        tempDF.write().mode("overwrite").parquet(parquetFile);
        //spark.stop();







    }
}
