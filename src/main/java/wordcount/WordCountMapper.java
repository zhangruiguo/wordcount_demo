package wordcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text,IntWritable>  {

    @Override
    public void map(LongWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();
        if(line != null) {
            String[] words = line.split("\t");
            for(String word:words) {
            	context.write(new Text(word), new IntWritable(1));
            }
        }
        
    }

}