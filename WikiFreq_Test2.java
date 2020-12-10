import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeSet;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import java.util.TreeSet;


public class WikiFreq_Test2 {
	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration());
		job.setJarByClass(WikiFreq_Test2.class);
		if(args.length != 2) {
			System.err.println("Useage <inputFile> <outputDir>");
			System.exit(2);
		}
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(WikiFreq_Mapper.class);
		job.setCombinerClass(WikiFreq_Combiner.class);
		job.setReducerClass(WikiFreq_Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
        if(status){
        	System.exit(0);
        }
        else{
        	System.exit(1);
        }
	}

	public static class Map extends WikiFreq_Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {

			String empty = " ";
			String comma = ",";

			String[] line = text.toString().split(empty);
			for(int i=1;i<line.length;i++){
				String temp = line[i-1].toLowerCase()+comma+line[i].toLowerCase();

				context.write(new Text(temp), new Text("1"));
			}
		}
	}

	private static class Merge extends WikiFreq_Combiner<Text, Text, Text, Text> {
		public void reduce(Text text, Iterable<Text> iterableValues, Context context) throws IOException, InterruptedException {

			int counter = 0;
			for (Text value : iterableValues) {
				counter = counter + Integer.parseInt(value.toString());
			}
			context.write(text, new Text(String.valueOf(counter)));
		}
	}

	public static class Reduce extends WikiFreq_Reducer<Text, Text, Text, Text> {

		TreeSet<pair_of_words> tree = new TreeSet<pair_of_words>();
		double totalC = 0;

		public void reduce(Text text, Iterable<Text> iteratorValue, Context context) throws IOException, InterruptedException {
			int count = 0;
			String empty = ".*\\\\*";
			String comma = ",";

			for (Text valueIn : iteratorValue){
				count = count + Integer.parseInt(valueIn.toString());
			}

			if (text.toString().matches(regex)){
				totalC = count;
			}
			else{
				String[] pairOfWords = text.toString().split(comma);
				tree.add(new pair_of_words(count/totalC, pairOfWords[0], pairOfWords[1]));

				if (tree.size() > 100){
					tree.pollFirst();
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			while (!tree.isEmpty()){
				pair_of_words pairs = tree.pollLast();
				context.write(new Text(pairs.key), new Text(pairs.value));
			}
		}
	}
}

class pair_of_words implements Comparable<pair_of_words> {
	double freq;
	String key;
	String value;

	pair_of_words(double freq, String key, String value){
		this.freq = freq;
		this.key = key;
		this.value = value;
	}

	public int compareTo(pair_of_words Pair_of_words){
		if (this.freq >= Pair_of_words.freq){
			return 1;
		}
		else{
			return -1;
		}
	}
}
