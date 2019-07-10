import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object SparkTest {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Test").getOrCreate()
    import spark.implicits._

    val input_path = args(0)
    val output_path = args(1)


    var df = spark.read.format("csv").option("header","true").load(input_path)


    def positiveValidation(x: String): Integer = {
      if(x=="SALESMAN")
        return 1
      else
        return 0
    }

    val positiveValidationUDF = udf(positiveValidation _)

    var dfA = df.withColumn("validation", positiveValidationUDF(col("designation")))

    dfA.write.format("com.databricks.spark.csv").option("header","true").save(output_path)

  }

}