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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountReviews {
	public static class AnimeReviewMapper extends Mapper<Object, Text, Text, IntWritable>{
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
			Hashtable<String, List<Double>> animeDict = new Hashtable<String, List<Double>>();
			String animeKey;
			Double animeScore;
			int nAnime = 0;
			for(String line:lines) {
				values = line.split(",");
				animeKey = values[3];
				animeScore = Double.parseDouble(values[8]);
				nAnime += 1;
				if(animeDict.containsKey(animeKey)) {
					animeDict.get(animeKey).add(animeScore);
				}
				else {
					List<Double> scores = new ArrayList<Double>();
					scores.add(animeScore);
					animeDict.put(animeKey, scores);
				}
			}
			Set<String> setOfAnimes = animeDict.keySet();
			for(String anime:setOfAnimes) {
				List<Double> scores = animeDict.get(anime);
				int l = scores.size();
				for(int i = 0; i < l; i++) {
					output.write(new Text(anime), one);
				}
			}
		}
	}

	public static class AnimeReviewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
		String inputLocation = args[0];
		String outputLocation = args[1];

		for (String s: args) {
			System.out.println(s);
		}

		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(AnimeReviewMapper.class);
	    job.setCombinerClass(AnimeReviewReducer.class); // in this case a combiner is possible!
	    job.setReducerClass(AnimeReviewReducer.class);
	     
	    //job.setJarByClass(CountWords.class);
		//job.waitForCompletion(true);
	}
}
