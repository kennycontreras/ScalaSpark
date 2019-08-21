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

object SplitDataPrediction {

  def main(args: Array[String]) {

    val spark =  SparkSession.
                    builder()
                    .config("spark.ui.showConsoleProgress", "true")
                    .getOrCreate() 
                    
    import spark.implicits._

    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val path = args(0)
    val bucketOutput = args(1)
    val country = args(2)
    val dataType = args(3)
    val dateData = args(4)
    
    var pathPrediction  = bucketOutput+"data_prediction/" +dateData+ "/" +country+ "/" +dataType+ "/"
    println(s"\nPath Prediction: ${pathPrediction} ")

    var dfPrediction = spark.read.format("avro").option("header", "true").load(path)

    println("Splitting data. This may take a moment...")
    dfPrediction.repartition(col("category_id_hashed")).write.mode("overwrite").partitionBy("category_id_hashed").option("header", "true").option("delimiter", ";").format("csv").save(pathPrediction)
    println("\nDone!\n")
  }
}
