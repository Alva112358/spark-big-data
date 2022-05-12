import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object YelpCheckinAnalysis {
  def main(args: Array[String]): Unit = {
      if (args.length < 2) {
          System.err.println("Usage: CheckinAnalysis <file>")
          System.exit(1)
      }

      val inPath = args(0)
      val outPath = args(1)
      val spark = SparkSession
         .builder
         .appName("YelpCheckinAnalysis")
         .getOrCreate()

      //val sc = new SparkContext();
      val schema = new StructType().add("business_id",StringType).add("date",StringType)
      val fileDF = spark.read.text(inPath)

      val dfJSON = fileDF.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*")
      val checkinAnalysis = dfJSON.withColumn("date",explode(split(col("date"),",")))
      checkinAnalysis.write.format("parquet").mode("overwrite").option("compression", "snappy").save(outPath)
  }
}
