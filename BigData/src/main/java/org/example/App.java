package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws Exception
    {
        Path input_dir = new Path("Mutiara/input");
        Path output_dir = new Path("Mutiara/output");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job atletMedalJob = Job.getInstance(conf, "MapReduce Data Atlet & jumlah medali");
        atletMedalJob.setJarByClass(App.class);
        atletMedalJob.setMapperClass(MapperJob.class);
        atletMedalJob.setReducerClass(ReducerJob.class);
        atletMedalJob.setMapOutputKeyClass(Text.class);
        atletMedalJob.setMapOutputValueClass(Text.class);
        atletMedalJob.setOutputKeyClass(NullWritable.class);
        atletMedalJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(atletMedalJob, input_dir);
        FileOutputFormat.setOutputPath(atletMedalJob, output_dir);
        atletMedalJob.waitForCompletion(true);
    }
}
