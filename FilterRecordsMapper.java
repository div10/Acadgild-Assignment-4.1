package acadgild.assign4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterRecordsMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split("\\|");
		if(!line[0].equals("NA") && !line[1].equals("NA")){
			context.write(value,null);
			System.out.println("Value = "+value);
		}
	}

}
