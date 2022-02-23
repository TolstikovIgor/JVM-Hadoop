import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class WordCount {

    public static void main(String[] args) {
        final String input = args[0];
        final String output = args[1];

        SparkSession sqlc = SparkSession.builder().getOrCreate();

        Dataset<Row> df = sqlc.read().option("header", "true").csv(input);

        Dataset<Row> wc = df
                .select(concat_ws(" ", col("class"), col("comment")).as("docs"))
                .select(split(col("docs"), "\\s").as("words"))
                .select(explode(col("words")).as("word"))
                .groupBy("word").count();
        wc.write().mode(SaveMode.Overwrite).csv(output);
        sqlc.stop();
    }
}

