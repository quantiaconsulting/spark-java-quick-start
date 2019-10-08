# spark-java-quick-start
Examples to start using spark with Java

##Project structure
###Root Package (com.quantiaconsulting.sjqs)
In the various sub-packages you can find the exercises to start using spark with the java APIs

###Solutions Package (com.quantiaconsulting.sjqs.solutions)
In this package you can find the solutions to the exercises.

##Run the solutions
You can directly run the classes using e spark-submit.

Instructions:

* Install spark 2.4.4
	* [download spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)
	* unzip it on your system
* Clone the repository
* Go to the pom folder and compile the project (`mvn package`)
* **Move the resources folder into the new target folder** (the resources must be in the same folder of the jar)
* Go to to the spark home folder (the main folder created during the unzip operations)
* Run the `spark-submit` on a class of your choice

```
./bin/spark-submit \
  --class com.quantiaconsulting.sjqs.SparkHelloWorld \
  --master local[*] \
  <absolute path of your jar>/sjqs-1.0.jar
```

* List of the class **ready to be run**:
	*com.quantiaconsulting.sjqs.SparkHelloWorld
	* com.quantiaconsulting.sjqs.RDD 
	* com.quantiaconsulting.sjqs.solutions.DF.Ingestion_CSV_noschema
	* com.quantiaconsulting.sjqs.solutions.DF.Ingestion_CSV_InferSchema
	* com.quantiaconsulting.sjqs.solutions.DF.Ingestion_CSV_WithSchema
	* com.quantiaconsulting.sjqs.solutions.DF.Ingestion_Parquet
	* com.quantiaconsulting.sjqs.solutions.DF.Preparation_DF_Basics
	* com.quantiaconsulting.sjqs.solutions.DF.Preparation_DF_Advanced1
	* com.quantiaconsulting.sjqs.solutions.DF.Preparation_DF_Advanced2
	* com.quantiaconsulting.sjqs.solutions.DF.Preparation_DF_Advanced3
	* com.quantiaconsulting.sjqs.solutions.DF.Preparation_DF_Advanced4
	* com.quantiaconsulting.sjqs.solutions.ML.BikeSharing
	* com.quantiaconsulting.sjqs.solutions.ML.NLP



