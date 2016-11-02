package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [conf<in>...] <out>");
			System.exit(2);
		}
//		conf.set("mapreduce.output.textoutputformat.separator", "\001");
		// 用于输入目录的子目录递归
//		conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(5);
		// 设置输入数据路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// 设置输出数据路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[1]))) {
			fs.delete(new Path(otherArgs[1]), true); // 如果目录存在,先删除
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

}