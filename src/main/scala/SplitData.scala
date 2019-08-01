import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object SplitData {

  def main(args: Array[String]) {

    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    // se comprueba si hay argumentos
    if(args.length == 0) {
      println("Por favor ingresar parametros")
    }

    val PathTrain = args(0)
    val OutputTrain = args(1)

    var dfTrain = spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter", ";").load(PathTrain)

    dfTrain = dfTrain.withColumn("CATEGORY", col("category_id"))
    dfTrain.write.mode("overwrite").partitionBy("CATEGORY").option("header", "true").option("delimiter", ";").format("com.databricks.spark.csv").save(OutputTrain)

  }

}
