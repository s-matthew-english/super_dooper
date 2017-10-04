import java.util.Map;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import org.apache.log4j.Logger;
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

import org.apache.hadoop.io.IOUtils;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.lang.ProcessBuilder;
import java.util.regex.*;



public class WorkingPrototype {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // deek logger
    private final Logger LOG = org.apache.log4j.Logger.getLogger(this.getClass());

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      // output data struct
      List<Map.Entry<String,Integer>> pairList = new ArrayList<>();

      StringTokenizer itr = new StringTokenizer(value.toString());
      LOG.warn("value.toString(): " + value.toString());


      while (itr.hasMoreTokens()) {
          try {
              // command execution
              Runtime rt = Runtime.getRuntime();
              String evmDir = "/home/ubuntu/go/src/github.com/ethereum/go-ethereum/build/bin/evm";
              String command = evmDir + " --debug --code " + value.toString() + " run";
              Process proc = Runtime.getRuntime().exec(command);
              LOG.warn(command);

              BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
              BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

              // define and initialise the ArrayList
              ArrayList<String> consoleOutput = new ArrayList<String>();
              ArrayList<String> debugOutput   = new ArrayList<String>();

              String s = null;
              while ((s = stdInput.readLine()) != null) {
                  consoleOutput.add(s);
              }
              while ((s = stdError.readLine()) != null) {
                  debugOutput.add(s);
              }

              for (String p : consoleOutput) {
                  LOG.warn(p);
                  Pattern pattern = Pattern.compile("([A-Za-z]+)([ \t]+)(\\d+)");
                  Matcher matcher = pattern.matcher(p);
                  while (matcher.find()) {
                      String opcodeTicker = matcher.group(1);
                      Integer stepTime = Integer.valueOf(matcher.group(3));

                      pairList.add(new AbstractMap.SimpleEntry<>(opcodeTicker, stepTime));
                  }
              }
          } catch (IOException e) {
              LOG.warn(e);
              LOG.warn("Exception Encountered!");
          }

          for (Map.Entry<String, Integer> entry : pairList) {
              String ki = entry.getKey().toString();
              Integer vi = entry.getValue();
              LOG.warn("opcode: " + ki + ", execution: " + vi);
          }
          // this is why it always starts at 'null', this itr.nextToken()
          // thing, separates over- whitespace or something? look it up.
          word.set(itr.nextToken());
          // into this context thing, maybe I can write the name of the file and an array
          // of the execution times
          context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    // deek logger
    private final Logger LOG = org.apache.log4j.Logger.getLogger(this.getClass());

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


      LOG.warn("***********");
      LOG.warn(values);
      LOG.warn("***********");

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "prototype: 03");
    job.setJarByClass(WorkingPrototype.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
