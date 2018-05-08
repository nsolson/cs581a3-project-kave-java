package kave.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompletionEventSelectionCountReducer extends Reducer<Text, EventsWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<EventsWritable> values, Context context)
			throws IOException, InterruptedException {

		int minSelected = Integer.MAX_VALUE;
		int maxSelected = Integer.MIN_VALUE;
		List<Integer> selected = new ArrayList<Integer>();
		int sum = 0;
		int total = 0;
		for (EventsWritable value : values) {

			String[] extraInfo = value.getExtraInfo();
			if (extraInfo.length == 4) {

				int selections = Integer.parseInt(extraInfo[3]);

				if (selections < minSelected) {
					minSelected = selections;
				}

				if (selections > maxSelected) {
					maxSelected = selections;
				}

				selected.add(selections);
				sum += selections;
				++total;
			}
		}

		context.write(new Text("Min Selected"), new Text("" + minSelected));
		context.write(new Text("Max Selected"), new Text("" + maxSelected));
		context.write(new Text("Avg Selected"), new Text("" + ((double) sum / (double) total)));
		Collections.sort(selected);
		context.write(new Text("Median Selected"), new Text("" + selected.get(selected.size() / 2)));
	}

}
