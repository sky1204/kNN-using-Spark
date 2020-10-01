import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Knn {
	public static class DoubleString implements WritableComparable<DoubleString> {
		private Double distance = 0.0;
		private String model = null;

		@Override
		public void readFields(DataInput in) throws IOException {//Reading data
			// TODO Auto-generated method stub
			distance = in.readDouble();
			model = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {//writing the data
			// TODO Auto-generated method stub
			out.writeDouble(distance);
			out.writeUTF(model);

		}

		@Override
		public int compareTo(DoubleString o) {
			// TODO Auto-generated method stub
			return (this.model).compareTo(o.model);
		}

		public void set(Double lhs, String rhs) {
			// TODO Auto-generated method stub
			distance = lhs;
			model = rhs;
		}

		public Double getDistance() {
			return distance;
		}

		public String getModel() {
			return model;
		}

	}

	public static class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleString> {//Mapper calculating the distance between the test data and training data 
		DoubleString distanceAndModel = new DoubleString();
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		Job conf;

		public void configure(Job job) {
			this.conf = job;
		}

		// Declaring some variables which will be used throughout the mapper
		int K;
		
		double normalisedRB;
		double normalisedRG;
		double normalisedRR;

		// The known ranges of the dataset, which can be hardcoded in for the purposes
		// of this example
		double minB = 0;
		double maxB = 255;
		double minG = 0;
		double maxG = 255;
		double minR = 0;
		double maxR = 255;

		private double normalisedDouble(String n1, double minValue, double maxValue) {//normalizing the distance using maximum and minimum values
			return (Double.parseDouble(n1) - minValue) / (maxValue - minValue);
		}

		private double squaredDistance(double n1) {
			return Math.pow(n1, 2);//method for calculating the square of the distance
		}

		private double totalSquaredDistance(double B1, double G1, double R1, double B2, double G2, double R2) {
			double BDiff = B1 - B2;
			double GDiff = G1 - G2;
			double RDiff = R1 - R2;

			// The sum of squared distances is used rather than the euclidean distance
			// because taking the square root would not change the order.
			// Status and gender are not squared because they are always 0 or 1.
			return squaredDistance(BDiff) + squaredDistance(GDiff) + squaredDistance(RDiff);//total squared distance for B.G,R
		}

		protected void setup(Context context) throws IOException, InterruptedException {

			Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String knnParams = "";

			K = context.getConfiguration().getInt("K", 495);
			System.out.println("K is" +K);
			if (stopWordsFiles != null && stopWordsFiles.length > 0) {
				System.out.println("In setup length" + stopWordsFiles.length);

				// only one test data file
				for (Path stopWordFile : stopWordsFiles) {
					System.out.println("stopWordFile for loop  " + stopWordFile);
					knnParams = knnParams + readFile(stopWordFile);
				}

				StringTokenizer st = new StringTokenizer(knnParams);
				System.out.println(st.countTokens() + " total tokens");
				// Using the variables declared earlier, values are assigned to K and to the
				// test data set, S.
				// These values will remain unchanged throughout the mapper
				
				normalisedRB = normalisedDouble(st.nextToken(), minB, maxB);
				normalisedRG = normalisedDouble(st.nextToken(), minG, maxG);
				normalisedRR = normalisedDouble(st.nextToken(), minR, maxR);
			}
		}

		private String readFile(Path filePath) {
			System.out.println("inside readfile " + filePath);
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));//reading the file
				String stopWord = null;
				String res = "";
				while ((stopWord = bufferedReader.readLine()) != null) {
					System.out.println("inside while");
					res = res + stopWord;
				}
				return res;
			} catch (IOException ex) {
				System.err.println("Exception while reading stop words file: " + ex.getMessage());
				return "";
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Tokenize the input line (presented as 'value' by MapReduce) from the csv file
			// This is the training dataset, R
			String rLine = value.toString();
			System.out.println(rLine);
			StringTokenizer st = new StringTokenizer(rLine);

			double normalisedSB = normalisedDouble(st.nextToken(), minB, maxB);
			double normalisedSG = normalisedDouble(st.nextToken(), minG, maxG);
			double normalisedSR = normalisedDouble(st.nextToken(), minR, maxR);
			String rModel = st.nextToken();

			// Using these row specific values and the unchanging S dataset values,
			// calculate a total squared
			// distance between each pair of corresponding values.
			double tDist = totalSquaredDistance(normalisedSB, normalisedSG, normalisedSR, normalisedRB, normalisedRG,
					normalisedRR);//calculating the total distance

			// Add the total distance and corresponding  model for this row into the
			// TreeMap with distance
			// as key and model as value.
			KnnMap.put(tDist, rModel);
			// Only K distances are required, so if the TreeMap contains over K entries,
			// remove the last one
			// which will be the highest distance number.
			if (KnnMap.size() > K) {
				KnnMap.remove(KnnMap.lastKey());//if the size is greater than k then remove the last element
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Loop through the K key:values in the TreeMap
			for (Map.Entry<Double, String> entry : KnnMap.entrySet()) {
				Double knnDist = entry.getKey();
				String knnModel = entry.getValue();
				// distanceAndModel is the instance of DoubleString declared aerlier
				distanceAndModel.set(knnDist, knnModel);
				// Write to context a NullWritable as key and distanceAndModel as value
				context.write(NullWritable.get(), distanceAndModel);
			}
		}
	}

	public static class KnnReducer extends Reducer<NullWritable, DoubleString, NullWritable, Text> {
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		int K ;

		@Override
		// setup() again is run before the main reduce() method
		protected void setup(Context context) throws IOException, InterruptedException {
			
			K = context.getConfiguration().getInt("K", 495);			
		}


		public void reduce(NullWritable key, Iterable<DoubleString> values, Context context)
				throws IOException, InterruptedException {
			// values are the K DoubleString objects which the mapper wrote to context
			// Loop through these
			for (DoubleString val : values) {
				String rModel = val.getModel();
				double tDist = val.getDistance();

				// Populate another TreeMap with the distance and model information extracted
				// from the
				// DoubleString objects and trim it to size K as before.
				KnnMap.put(tDist, rModel);
				if (KnnMap.size() > K) {
					KnnMap.remove(KnnMap.lastKey());
				}
			}

			// This section determines which of the K values (models) in the TreeMap occurs
			// most frequently
			// by means of constructing an intermediate ArrayList and HashMap.

			// A List of all the values in the TreeMap.
			List<String> knnList = new ArrayList<String>(KnnMap.values());

			Map<String, Integer> freqMap = new HashMap<String, Integer>();

			// Add the members of the list to the HashMap as keys and the number of times
			// each occurs
			// (frequency) as values
			for (int i = 0; i < knnList.size(); i++) {
				Integer frequency = freqMap.get(knnList.get(i));
				if (frequency == null) {
					freqMap.put(knnList.get(i), 1);
				} else {
					freqMap.put(knnList.get(i), frequency + 1);
				}
			}

			// Examine the HashMap to determine which key (model) has the highest value
			// (frequency)
			String mostCommonModel = null;
			int maxFrequency = -1;
			for (Map.Entry<String, Integer> entry : freqMap.entrySet()) {
				if (entry.getValue() > maxFrequency) {
					mostCommonModel = entry.getKey();
					maxFrequency = entry.getValue();
				}
			}

			// Finally write to context another NullWritable as key and the most common
			// model just counted as value.
			context.write(NullWritable.get(), new Text("The color which is most repeated is " +mostCommonModel)); // Use this line to produce a single
																			// classification
			context.write(NullWritable.get(), new Text(KnnMap.toString())); // Use this line to see all K nearest
																			// neighbours and distances
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// Create configuration

		Configuration conf = new Configuration();

		if (args.length != 4) {
			System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
			System.exit(2);
		}
		conf.setInt("K", Integer.parseInt(args[3]));

		
		// Create job
		Job job = new Job(conf);
		job.setJobName("Find K-Nearest Neighbour");
		job.setJarByClass(Knn.class);
		
		// job.
		// Set the third parameter when running the job to be the parameter file and
		// give it an alias
		// job.addCacheFile(new URI(args[2] + "#knnParamFile")); // Parameter file
		// containing test data
		DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());

		// Setup MapReduce job
		job.setMapperClass(KnnMapper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(1); // Only one reducer in this design

		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}