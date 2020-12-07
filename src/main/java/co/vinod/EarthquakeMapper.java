package co.vinod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarthquakeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
	
		String []parts = value.toString().split(",");
		
		// ignore invalid lines/records
		if(parts.length!=12) {
			System.err.println("parts.lentgh is incorrect: " + parts.length);
			return;
		}
		
		// output key is the name of the region
		String outKey = parts[11];
		
		
		// output value is the magnitude of Earthquake
		double outValue = Double.parseDouble(parts[8]);
		
		// record the output in the context object
		context.write(new Text(outKey), new DoubleWritable(outValue));
	}
	

}
