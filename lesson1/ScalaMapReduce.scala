import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text};
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer};


object ScalaMapReduce extends Configured with Tool {
  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = ???
}

class SwapMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  val word = new Text()
  val amount = new IntWritable()

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
    val kv = value.toString.split("\\s", 2)
    word.set(kv(0))
    amount.set(kv(1).toInt)
    context.write(amount, word)
  }
}