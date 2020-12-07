package co.vinod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarthquakeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		double maxMagnitude = Double.MIN_VALUE;
		for (DoubleWritable value : values) {
			maxMagnitude = Math.max(maxMagnitude, value.get());
		}

		context.write(key, new DoubleWritable(maxMagnitude));
	}

}
