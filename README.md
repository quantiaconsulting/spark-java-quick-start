# spark-java-quick-start
Examples to start using spark with Java
**Note**: Spark runs on Java 8, make sure to have it installed on your system

## Project structure
### Root Package (com.quantiaconsulting.sjqs)
In the various sub-packages you can find the exercises to start using spark with the java APIs

### Solutions Package (com.quantiaconsulting.sjqs.solutions)
In this package you can find the solutions to the exercises.

## Run the solutions
You can directly run the classes using e spark-submit.

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

* List of the class **ready to be run**:
	*com.quantiaconsulting.sjqs.basics.codeBrowsing.SparkHelloWorld
	* com.quantiaconsulting.sjqs.basics.codeBrowsing.RDD 
	* com.quantiaconsulting.sjqs.solutions.df.Ingestion_CSV_noschema
	* com.quantiaconsulting.sjqs.solutions.df.Ingestion_CSV_InferSchema
	* com.quantiaconsulting.sjqs.solutions.df.Ingestion_CSV_WithSchema
	* com.quantiaconsulting.sjqs.solutions.df.Ingestion_Parquet
	* com.quantiaconsulting.sjqs.solutions.df.Preparation_DF_Basics
	* com.quantiaconsulting.sjqs.df.codeBrowsing.Preparation_DF_Advanced1
	* com.quantiaconsulting.sjqs.df.codeBrowsing.Preparation_DF_Advanced2
	* com.quantiaconsulting.sjqs.df.codeBrowsing.Preparation_DF_Advanced3
	* com.quantiaconsulting.sjqs.df.codeBrowsing.Preparation_DF_Advanced4
	* com.quantiaconsulting.sjqs.solutions.ml.BikeSharing
	* com.quantiaconsulting.sjqs.solutions.ml.NLP
	
## Utils
* If you need to add additional repository to the maven pom please refer to:
	* https://maven.apache.org/guides/introduction/introduction-to-repositories.html
	* https://stackoverflow.com/questions/15429142/add-maven-repositories-for-a-project-in-eclipse


