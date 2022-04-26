package OriginalLog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class MaxLogTime {
public static void main(String [] args) throws Exception
{
Configuration c=new Configuration();
c.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path output=new Path(files[1]);
Job j=new Job(c,"maxTime");
j.setJarByClass(MaxLogTime.class);
j.setMapperClass(MapForWordCount.class);
j.setReducerClass(ReduceForWordCount.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(j, input);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
}
public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{
	String data = value.toString();
	String[] lines=data.split("\n");
	for(String line: lines ){
		String newLine = new String(line);
		String[] singleData = newLine.split(",");
		Text outputKey = new Text(singleData[0]);
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		try {
			Date time1 = format.parse(singleData[1]);
			Date time2 = format.parse(singleData[2]);
			int difference = (int) (time2.getTime() - time1.getTime());
			IntWritable outputValue = new IntWritable(difference);
			con.write(outputKey, outputValue);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
}
}

public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>{
	private Text MaxWord = new Text();
	private int max = 0;
	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable value : values){
			sum += value.get();
		}
		if(sum > max){
			max = sum;
			MaxWord.set(word);
		}
	}
	protected void cleanup (Context con) throws IOException, InterruptedException{
		con.write(new Text(MaxWord),new IntWritable(max));
	}

}

}


// movie
// https://my-learnings-about-hadoop.blogspot.com/2017/08/mapreduce-real-timeexamples.html