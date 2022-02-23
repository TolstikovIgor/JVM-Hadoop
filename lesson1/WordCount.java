import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    public static final String IN_PATH_PARAM = "wordcount.input";
    public static final String OUT_PATH_PARAM = "wordcount.output";

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private final Text text = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] words = line.split("\\s");
            for (String word: words) {
                text.set(word);
                context.write(text, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class AlphabetPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions == 1) {
                return 0; // all in one
            }

            return (key.getLength() > 2) ? 1 : 0;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);
        job.setPartitionerClass(AlphabetPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(getConf().get(IN_PATH_PARAM)));
        FileOutputFormat.setOutputPath(job, new Path(getConf().get(OUT_PATH_PARAM)));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(job.getCounters().toString());
        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
}
