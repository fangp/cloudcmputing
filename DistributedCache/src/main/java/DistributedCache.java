import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class DistributedCache {

    public static void main(String[] args) throws Exception {
          String temoutput = "temoutput/";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FindWords");
        job.setJarByClass(FindWords.class);
        job.setMapperClass(FindWords.mapper.class);
        job.setCombinerClass(FindWords.reducer.class);
        job.setReducerClass(FindWords.reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(temoutput));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        if(job.waitForCompletion(true)){
              Configuration conf2 = new Configuration();
              Job job2 = Job.getInstance(conf2, "FindFrequency");
              job2.addCacheFile(new URI("hdfs://ec2-18-221-80-249.us-east-2.compute.amazonaws.com:8020/user/ubuntu/"+temoutput+"part-r-00000#words"));
              job2.setJarByClass(FindFrequency.class);
              job2.setMapperClass(FindFrequency.mapper.class);
              job2.setCombinerClass(FindFrequency.reducer.class);
              job2.setReducerClass(FindFrequency.reducer.class);
              job2.setOutputKeyClass(Text.class);
              job2.setOutputValueClass(IntWritable.class);
              FileInputFormat.addInputPath(job2, new Path(args[1]));
              FileOutputFormat.setOutputPath(job2, new Path(args[2]));
              System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        else
            System.exit(1);
    }
}