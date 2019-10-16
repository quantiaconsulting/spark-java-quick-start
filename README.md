# spark-java-quick-start
Examples to start using spark with Java
**Note**: Spark runs on Java 8, make sure to have it installed on your system

## Project structure
### Root Package (com.quantiaconsulting.sjqs)
In the various sub-packages you can find the exercises to start using spark with the java APIs

### Solutions Package (com.quantiaconsulting.sjqs.solutions)
In this package you can find the solutions to the exercises.

## Utils

## Run the solutions

### spark-submit
The spark-submit script is used to launch applications on a cluster.

[doc](https://spark.apache.org/docs/latest/submitting-applications.html)

```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```
The most common options are:

* --class: The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
* --master: The master URL for the cluster (e.g. spark://23.195.26.187:7077)
* --deploy-mode: Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) (default: client)
* --conf: Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes (as shown).
application-jar: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes.
application-arguments: Arguments passed to the main method of your main class, if any.

Instructions:

* Install spark 2.4.4
	* [download spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)
	* unzip it on your system
* Clone the repository
* Go to the pom folder and compile the project (`mvn package`)
* **Copy the resources folder into the new target folder** (the resources must be in the same folder of the jar)
`cp -r resources/ target/resources/`
* Go to to the spark home folder (the main folder created during the unzip operations)
* Run the `spark-submit` on a class of your choice

```
./bin/spark-submit \
  --class com.quantiaconsulting.sjqs.basics.codeBrowsing.SparkHelloWorld \
  --master local[*] \
  <absolute path of your jar>/sjqs-1.0.jar
```

**Note**: Make sure to use absolute path for `<<your path>>`
	
### Maven additional repository
* If you need to add additional repository to the maven pom please refer to:
	* https://maven.apache.org/guides/introduction/introduction-to-repositories.html
	* https://stackoverflow.com/questions/15429142/add-maven-repositories-for-a-project-in-eclipse

## Spark Introduction
[spark](http://emanueledellavalle.org/slides/spark/spark.html#1)

## First Application - Word Count

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/basics/codeBrowsing/SparkWordCount.java)

### Java Code complexity

Focus on the complexity of the java code if compared to python: 

* `words.mapToPair(s -> new Tuple2<>(s, 1))` vs. `.map(lambda s: (s, 1))`
* `mapToPair` vs. `map new Tuple2<>(s, 1) vs (s, 1)`

### Collect for RDD
[doc](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

`counts.collect()` only works if the counts RDD is **small enough** to fit the driver memory and if the communication channel is https://

## Data Ingestion
### CSV
#### Without schema
[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Ingestion_CSV_noschema.java)

Focus on `.option("header",true)`

#### Automatically infer schema
[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Ingestion_CSV_InferSchema.java)

Focus on `.option("inferSchema",true)`

#### Manually specify schema
[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Ingestion_CSV_WithSchema.java)

#### Challenge
**Instructions:** 

* Use the class [code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/challenges/Ingestion_CSV.java) to start
* Read the file `/resources/2015_02_clickstream.tsv` and assign it to a DataFrame named testDF with the right schema using what you have learned.

**Data Schema:**

* prev_id: integer
* curr_id: integer
* n: integer
* prev_title: string
* curr_title: string
* type: string 

### Parquet

Parquet is a free and open-source column-oriented data storage format, it is a part of apache-hadoop ecosystem -> [doc](https://parquet.apache.org/documentation/latest/)

A Parquet file already contains metadata with the data schema, the number of rows, etc.

Read a parquet file as Dataframe -> `spark.read().parquet(<parquet file path>);` 

### Wrap-up

* CSV Ingestion 
	* No schema: all the fields is considered as `String` 
	* Infer schema: the correct data types are inferred by the system, but the infer operation costs a double scan of the file
	* manually provided schema: correct data types and single scan
* Parquet Ingestion: Columnar format, no need for schema specification


## Data Exploration and Preparation

### Basics
[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Preparation_DF_Basics.java)

Focus on:

* Filter

```
Dataset<Row> partialres = tempDF.where("requests >= 1000");
System.out.println(partialres.count() + " rows with requests >= 1000 ");

```
* Projection

```
Dataset<Row> partialres = tempDF.where("requests >= 1000").select("timestamp","requests");
```

* Order

```
Dataset<Row> partialres3 = partialres2.orderBy("requests");
```

Or using the `col` static class
```
import static org.apache.spark.sql.functions.col;
Dataset<Row> partialres3 = partialres2.orderBy(col("requests").desc_nulls_last());
```

* Using tempview and SQL

```
tempDF.createOrReplaceTempView("richieste");
Dataset<Row> sqlRes = spark.sql("SELECT timestamp, requests FROM richieste WHERE requests >= 1000 ORDER BY
```

* Physical Plan

```
== Physical Plan ==
*(2) Sort [requests#2 DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(requests#2 DESC NULLS LAST, 200)
   +- *(1) Filter (isnotnull(requests#2) && (requests#2 >= 1000))
      +- InMemoryTableScan [timestamp#0, requests#2], [isnotnull(requests#2), (requests#2 >= 1000)]
            +- InMemoryRelation [timestamp#0, site#1, requests#2], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) FileScan parquet [timestamp#0,site#1,requests#2] Batched: true, Format: Parquet, Location: InMem
```

**Note**: Using the DataFrame API and SQL produce the same physical plan

### Advanced - Columns and Unix Timestamp

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Preparation_DF_Advanced1.java)

Focus On:

* Add new columns: use `withColumn(...)` -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#withColumn-java.lang.String-org.apache.spark.sql.Column-)
* Transform a date/datetime in Unix Timestamp: use `unix_timestamp(...)` -> [doc](https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/functions.html)

### Advanced - Date Functions

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Preparation_DF_Advanced2.java)

Focus On:

* More Functions! Extract year and monto from a date/datetime: use `year(...)` and `montg(...)` -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#withColumn-java.lang.String-org.apache.spark.sql.Column-)

### Advanced - Group and Date Format

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Preparation_DF_Advanced3.java)

Focus On:

* Group and aggregate: use `groupBy(...)` and `agg(...)` -> [groupBy-doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#groupBy-org.apache.spark.sql.Column...-), [agg-doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#agg-org.apache.spark.sql.Column-org.apache.spark.sql.Column...-)
* Format date: use `date_format(...)` -> [doc](https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/functions.html#date_format(org.apache.spark.sql.Column,%20java.lang.String)), [java date format doc](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html)

### Advanced - Join

#### Join Intro
[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/codeBrowsing/Preparation_DF_Advanced4.java)

[doc](https://www.db-book.com/db7/slides-dir/PDF-dir/ch2.pdf) from slide 2.16 a slide 2.20

[doc](https://www.db-book.com/db7/slides-dir/PDF-dir/ch4.pdf) from slide 4.3 to slide 4.21

Focus On:

* Join DataFrames: `join(...)` -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#join-org.apache.spark.sql.Dataset-)

### Challenge

**Instructions:** 

* Use the class [code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/df/challenges/Ingestion_Preparation_CSV_Exercise.java) to start
* Read the file `/resources/2015_02_clickstream.tsv` and assign it to a DataFrame named testDF with the right schema using what you have learned.
* Find the top 10 most used path with 2 jumps

**Hints:** 

* Read the same file in two different dataframes (create the schema manually)
* Rename the columns of one of the two Dataframes (use `.withColumnRenamed(...)` -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html#withColumnRenamed-java.lang.String-java.lang.String-))
* Join the two Dataframes using the `curr_id` of the original Dataframes and the `prev_id` of the renamed one
* sum the number of clicks of the two paths (the value in the column `n` of the original Dataframe + the value in the column `n` of the renamed Dataframe). Try to use `expr(...)` -> [doc](https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/functions.html)
* order desc by the sum

**Data Schema:**

* prev_id: integer
* curr_id: integer
* n: integer
* prev_title: string
* curr_title: string
* type: string 

## Machine Learning

### ML intro
![](./resources/img/01.png)
![](./resources/img/02.png)
![](./resources/img/03.png)
![](./resources/img/04.png)
![](./resources/img/05.png)

### A case of text analytics that uses a Decision Tree as classifier
![](./resources/img/06.png)
![](./resources/img/07.png)
![](./resources/img/08.png)

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/ml/codeBrowsing/NLP.java)

Focus on:

* RegexTokenizer ->[doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/feature/RegexTokenizer.html)
* StopWordsRemover -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/feature/StopWordsRemover.html)
* Binarizer -> [doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/feature/Binarizer.html)
* Add more stopwords:

```
String[] stopwords = remover.getStopWords();
ArrayList<String> stopwordsList = new ArrayList<String>(Arrays.asList(stopwords));
stopwordsList.add("br");
String[] newStopWords = new String[stopwordsList.size()];
newStopWords = stopwordsList.toArray(newStopWords);
remover = new StopWordsRemover()
    .setInputCol("tokens")
    .setOutputCol("stopWordFree")
    .setStopWords(newStopWords);
```

* DecisionTreeClassifier ->[doc](https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html)
* Max Depth -> [doc](https://spark.apache.org/docs/latest/mllib-decision-tree.html#classification)

### A case of predictive analytics using a Decision Tree or a Random Forest as a regressor

![](./resources/img/09.png)
![](./resources/img/10.png)

[code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/ml/codeBrowsing/BikeSharing.java)

### Challenge

**Instructions:** 

* Use the class [code](https://github.com/quantiaconsulting/spark-java-quick-start/blob/master/src/main/java/com/quantiaconsulting/sjqs/ml/challenges/ParkingMeteo.java) to start
* Read the file `/resources/pwJoin.parquet` and assign it to a DataFrame.
* Predict the number of free parking per hour:
	* Use at least two models of your choice
 	* Show that you understand hyper-parameter tuning

**Data Description:** The parquet file contains data related to a parking in Como and the weather during a period between 01/08/2016 and 01/05/2017.

**Data Schema:**

* temporalId:long
* parkingId:integer
* parkingName:string
* freeParking:double
* description:string
* icon:string
* temperature:double
* month:integer
* doy:integer
* dom:integer
* dow:string
* hour:integer

