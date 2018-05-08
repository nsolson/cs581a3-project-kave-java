package kave.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventsBySessionReducer extends Reducer<Text, EventsWritable, NullWritable, EventsWritable> {

	@Override
	public void reduce(Text key, Iterable<EventsWritable> values, Context context)
			throws IOException, InterruptedException {

		for (EventsWritable value : values) {
			context.write(NullWritable.get(), value);
		}
	}

}
