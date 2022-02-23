import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.Pipeline

object SentimentAnalysis {
  def main(args: Array[String]): Unit = {
    val trainCSV = args(0)
    val testCSV = args(1)
    val spark = SparkSession.builder().getOrCreate()

    import spark.sqlContext.implicits._


    val trainingDF = spark.read
      .option("header", "true")
      .option("quotes", "\"")
      .option("escape", "\"")
      .option("inferSchema", "true")
      .csv(trainCSV)

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val hashingTF = new HashingTF()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("features")
      .setNumFeatures(1000)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array[PipelineStage](tokenizer, remover, hashingTF, lr))

    val model = pipeline.fit(trainingDF)


    val testDF = spark.read
      .option("header", "true")
      .option("quotes", "\"")
      .option("escape", "\"")
      .option("inferSchema", "true")
      .csv(testCSV)

    val rowsCount = testDF.count()

    val predictionDF = model.transform(testDF)

    predictionDF.show(20)

    predictionDF
      .groupBy($"label" - $"prediction" as "result").count()
      .select($"count" / rowsCount as "accuracy")
      .filter($"result" === 0)
      .select($"accuracy")
      .show()

    spark.stop()
  }
}
