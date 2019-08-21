import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.joda.time.DateTime
import org.joda.time.Days
import java.sql.Timestamp
import java.util.Calendar
import org.apache.log4j._




/*
submit job on dataproc


gcloud dataproc jobs submit spark \
    --cluster test-cluster --region us-central1 \
    --properties spark.jars.packages='org.apache.spark:spark-avro_2.11:2.4.3' \
    --class "SplitData" \
    --jars /Users/StriderKeni/IdeaProjects/spark_test/target/scala-2.11/spark_test_2.11-1.0.jar \
    -- gs://bucket_testing_ml/pairwise/2019/06/30/CHL/000000000000.csv \
    gs://bucket_testing_ml/pairwise/2019/06/30/CHL/PLP/

submit job local mode

spark-submit --class "SplitData" \
--packages 'com.databricks:spark-avro_2.11:3.2.0' \
--master local /Users/StriderKeni/Downloads/scripts/SplitPairwise/splitpairwise_2.11-1.0.jar \
-- /Users/StriderKeni/Downloads/pairwise_2019_06_30_CHL_PLP_pairwise000000000000.csv \
/Users/StriderKeni/Downloads/scripts/ 

*/

object SplitDataAvro {

  def main(args: Array[String]) {

    val spark =  SparkSession.
                    builder()
                    .config("spark.ui.showConsoleProgress", "true")
                    .getOrCreate() 
                    
    import spark.implicits._


    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    if(args.length == 0) {
      println("Por favor ingresar parametros")
    }

    val PathTrain = args(0)
    val bucketOutput = args(1)
    val country = args(2)
    val dataType = args(3)
    val flag: Integer = args(4).toInt
    val dateData = args(5)
    

    var pathTrain  = bucketOutput+"data_training/" +dateData+ "/" +country+ "/" +dataType+ "/"
    var pathVal =  bucketOutput+"data_val/" +dateData+ "/" +country+ "/" +dataType+ "/"
    println(s"\nPath Training: ${pathTrain} ")
    println(s"Path Validation: ${pathVal} \n")

    var dfTrain = spark.read.format("avro").option("header", "true").load(PathTrain)

    println("\nCalculating max and min target_date...\n")
    val Row(minValueString: String, maxValueString: String) = dfTrain.agg(min("target_date"), max("target_date")).head
    println(s"Min Target Date: ${minValueString}\nMax Target Date: ${maxValueString}")

    // Parse String date to DateTime
    var maxValue: DateTime = DateTime.parse(maxValueString)
    var minValue: DateTime = DateTime.parse(minValueString)

    // var diff: Double = (maxValue.toLocal - minValue.getTime()) / (1000.0 * 60 * 60 * 24) * 0.2 
    var diff: Double = Days.daysBetween(minValue.toLocalDate(), maxValue.toLocalDate()).getDays() * 0.2
    var subsMax: DateTime = maxValue.minusDays(3) 

    def flagTrain = udf((date: String) => {
      var castDate: DateTime = DateTime.parse(date)
      if (castDate.isBefore(subsMax)) 1
      else 0
    } )

    println("\nAdding Flag column...")
    var dfFlagTrain = dfTrain.withColumn("flag", flagTrain(col("target_date")))

    println("Filtering by flag argument...")
    if (flag == 1) {
      var dfFlagVal = dfFlagTrain.where(col("flag")===1)  
      println("Writing Training Data...")
      println(s"Destination Folder: ${bucketOutput+"data_training/" +dateData+ "/" +country+ "/" +dataType+ "/"} \n")
      dfFlagVal.repartition(col("category_id_hashed")).write.mode("overwrite").partitionBy("category_id_hashed").option("header", "true").option("delimiter", ";").format("csv").save(bucketOutput+"data_training/" +dateData+ "/" +country+ "/" +dataType+ "/")  
    } else {
      dfFlagTrain = dfFlagTrain.where(col("flag")===0)
      println("Writing Validation Data...")
      println(s"Destination Folder: ${bucketOutput+"data_val/" +dateData+ "/" +country+ "/" +dataType+ "/"} \n")
      dfFlagTrain.repartition(col("category_id_hashed")).write.mode("overwrite").partitionBy("category_id_hashed").option("header", "true").option("delimiter", ";").format("csv").save(bucketOutput+"data_val/" +dateData+ "/" +country+ "/" +dataType+ "/")  
    }
    println("\nDone!\n")
  }
}
