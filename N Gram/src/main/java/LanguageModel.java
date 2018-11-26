import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {


		//input : I love USA\t10
		//output: key: I love   value: love=10

		int threshold; //filter the n-gram lower than thre shold
		@Override
		public void setup(Context context) {
			// how to get the threshold parameter from the configuration?
			Configuration config = context.getConfiguration();
			threshold = config.getInt("threshold", 20); // 20 is default value
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//I love USA\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			if (count < threshold) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]);
				sb.append(" ");
			}

			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;

			context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;
		// get the top K parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//Expected Output Key = I love
			//Expected Output Value = <USA=10, CANADA=10, BOOKS=20....etc> top k values

			//expected tm: <20, <BOOKS>, etc...>, <10, <USA, CANADA>>
			TreeMap<Integer, List<String>> tm = new TreeMap<>(Collections.<Integer>reverseOrder());
			for (Text val : values) {
				String value = val.toString().trim(); //val: USA=10;
				String word = value.split("=")[0];
				int count = Integer.parseInt(value.split("=")[1].trim());
				if (tm.containsKey(count)) {
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iter = tm.keySet().iterator();
			for (int j = 0; iter.hasNext() && j < topK; ) { // not need for j++
				int keyCount = iter.next() ;
				List<String> words = tm.get(keyCount);
				for (int i = 0; i < words.size() && j < topK; i++) {
					context.write(new DBOutputWritable(key.toString(), words.get(i), keyCount), NullWritable.get()); // reducer output must be key value pair
					j++;
				}
			}


		}
	}
}
