import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    // Map function processes each input record and emits key-value pairs
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Tokenize input text
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      // Get the name of the input file being processed
      String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();

      // Initialize counts for each GO term
      int count_GO_0030420 = 0;
      int count_GO_0045304 = 0;
      int count_GO_0045809 = 0;
      int count_GO_0045808 = 0;

      // Iterate through tokens and count occurrences of specific GO terms
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();

        // Check if the token is a GO term of interest
        if (token.equals("GO:0030420")) {
          count_GO_0030420++;
        } else if (token.equals("GO:0045304")) {
          count_GO_0045304++;
        } else if (token.equals("GO:0045809")) {
          count_GO_0045809++;
        } else if (token.equals("GO:0045808")) {
          count_GO_0045808++;
        }
      }

      // Emit counts for each GO term with filename as part of the key
      outputKey.set(fileName + "_GO:0030420");
      context.write(outputKey, new IntWritable(count_GO_0030420));

      outputKey.set(fileName + "_GO:0045304");
      context.write(outputKey, new IntWritable(count_GO_0045304));

      outputKey.set(fileName + "_GO:0045809");
      context.write(outputKey, new IntWritable(count_GO_0045809));

      outputKey.set(fileName + "_GO:0045808");
      context.write(outputKey, new IntWritable(count_GO_0045808));
    }
  }

  public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    // Reduce function sums up the counts for each key
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      // Emit the final count for each key
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    // Configuration for the Hadoop job
    Configuration conf = new Configuration();
    // Create a new job named "GO term count"
    Job job = Job.getInstance(conf, "GO term count");
    // Set the main class
    job.setJarByClass(WordCount.class);
    // Set the Mapper class
    job.setMapperClass(TokenizerMapper.class);
    // Use Combiner class to perform local aggregation before shuffling data to reducers
    job.setCombinerClass(IntSumReducer.class);
    // Set the Reducer class
    job.setReducerClass(IntSumReducer.class);
    // Set the output key class
    job.setOutputKeyClass(Text.class);
    // Set the output value class
    job.setOutputValueClass(IntWritable.class);
    // Set the input paths (multiple paths can be specified)
    FileInputFormat.addInputPaths(job, String.join(",", args[0]));
    // Set the output path
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // Exit the job with status code 0 if successful, 1 otherwise
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
