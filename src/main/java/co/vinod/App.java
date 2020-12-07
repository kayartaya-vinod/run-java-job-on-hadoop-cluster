package co.vinod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage co.vinod.App <input> <output>");
			System.exit(1);
		}

		// Create configuration
		Configuration conf = new Configuration(true);

		// create the job specification object
		Job job = new Job(conf, "Earthquake Measurement");
		job.setJarByClass(App.class);

		// Setup input and output paths
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outPath)) {
			hdfs.delete(outPath, true);
		}

		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		// set mapper and reducer classes
		job.setMapperClass(EarthquakeMapper.class);
		job.setReducerClass(EarthquakeReducer.class);

		// specify the output key and value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// wait for the job to finish before exiting
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
