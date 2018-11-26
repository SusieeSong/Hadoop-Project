import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram; // number of gram
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			line = line.trim().toLowerCase();

			//remove none-alphabetic words
			line.replaceAll("[^a-z]", " ");
			String[] words = line.split("\\s+");
			
			//build n-gram based on array of words
			if (words.length < 2) {
				return;
			}

			StringBuilder sb;
			for (int i = 0; i < words.length; i++) {
				sb = new StringBuilder();
				sb.append(words[i]);
				for (int j = 1; i + j < words.length && j < noGram; i++) {
					sb.append(" ");
					sb.append(words[i + j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
			}

		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

}