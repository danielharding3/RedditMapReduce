package dsProject3.dsProject3;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class RedditDataMapReduce {
	
	
	public static class MapClass extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		IntWritable imageID = new IntWritable();
		IntWritable interactions = new IntWritable();
		//IntWritable upvotes = new IntWritable();
		//IntWritable downvotes = new IntWritable();
		//IntWritable comments = new IntWritable();
		
		public void map(Object key, Text value, Mapper<Object, Text,
				IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
			//StringTokenizer strToken = new StringTokenizer(value.toString());
			String str = value.toString();
			String[] arr = str.split("\t");
			
			imageID.set(Integer.parseInt(arr[RedditConstants.IMAGE_ID]));
			
			int numUpvotes = Integer.parseInt(arr[RedditConstants.UPVOTES]);
			int numDownvotes = Integer.parseInt(arr[RedditConstants.DOWNVOTES]);
			int numComments = Integer.parseInt(arr[RedditConstants.COMMENTS]);
			int numInteractions = numUpvotes + numDownvotes + numComments;
			
			interactions.set(numInteractions);
			
			context.write(imageID, interactions);
			//Text wordOut = new Text();
			//IntWritable ones = new IntWritable(1);
			
//			while (strToken.hasMoreTokens()) {
//				wordOut.set(strToken.nextToken());
//				context.write(wordOut, ones);
//			}
		}
		
	}
	
	public static class ReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		public void reduce(IntWritable interactions, Iterable<IntWritable> imageIDs,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Set<Integer> imageIDSet = new HashSet<Integer>();
			
			for (IntWritable imageID : imageIDs) {
				imageIDSet.add(imageID.get());
			}
			
			IntWritable size = new IntWritable(imageIDSet.size());
			context.write(interactions, size);
			
		}
		
		/*
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while (iterator.hasNext()) {
				count++;
				iterator.next();
			}
			IntWritable output = new IntWritable(count);
			context.write(term, output);
		}
		*/
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Need only 2 files");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Interactions per Reddit Post");
		job.setJarByClass(RedditDataMapReduce.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}

}
