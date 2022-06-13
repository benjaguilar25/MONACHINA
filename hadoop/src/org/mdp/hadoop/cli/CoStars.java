package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mdp.hadoop.cli.CountWords.CountWordsMapper;
import org.mdp.hadoop.cli.CountWords.CountWordsReducer;

public class CoStars {
	public static class CoStarsMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final IntWritable one = new IntWritable(1);

		
		/**
		 * Makes a dictionary with the pairs (movieKey, actorList) with all the actors in that movie
		 * Then in makes every combination of pairs (without repeating itself) of the actor
		 * Finally it outputs (actor1#actor2, one)
		 * 
		 * @throws InterruptedException 
		 * 
		 */
		 @Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String content = value.toString();
			String[] lines = content.split("\n");
			String[] values;
			Hashtable<String, List<String>> movieDict = new Hashtable<String, List<String>>();
			String actorName, movieKey;
			int nMovies = 0;
			for(String line:lines) {
				values = line.split("\t");
				if(values[4]=="THEATRICAL_MOVIE") {
					actorName = values[0];
					nMovies += 1;
					movieKey = values[1] + "#" + values[2] + "#" + values[3];
					if(movieDict.containsKey(movieKey)) {
						movieDict.get(movieKey).add(actorName);
					}else {
						List<String> actors = new ArrayList<String>();
						actors.add(actorName);
						movieDict.put(movieKey, actors);
					}
				}
			}
			Set<String> setOfMovies = movieDict.keySet();
			for(String movie:setOfMovies) {
				List<String> actors = movieDict.get(movie);
				int l = actors.size();
				for(int i = 0; i < l; i++) {
					String actor = actors.get(i);
					for(int j = 1; j < l-i; j++) {
						output.write(new Text(actor + "#" + actors.get(i+j)), one);
					}
				}
			}
		}
	}

	public static class CoStarsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

			/**
			 * Given a key (actor1#actor2) and all values (partial counts) for 
			 * that key produced by the mapper, then we will sum the counts and
			 * output (word,sum)
			 * 
			 * @throws InterruptedException 
			 */
			@Override
			public void reduce(Text key, Iterable<IntWritable> values,
					Context output) throws IOException, InterruptedException {
				int sum = 0;
				for(IntWritable value: values) {
					sum += value.get();
				}
				output.write(key, new IntWritable(sum));
			}
		}

	
	/**
	 * Main method that sets up and runs the job
	 * 
	 * @param args First argument is input, second is output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+CountWords.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];
		
		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(CoStarsMapper.class);
	    job.setCombinerClass(CoStarsReducer.class); // in this case a combiner is possible!
	    job.setReducerClass(CoStarsReducer.class);
	     
	    job.setJarByClass(CountWords.class);
		job.waitForCompletion(true);
	}
}
