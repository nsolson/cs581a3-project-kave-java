package kave.mapreduce;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import kave.Constants;

public class CompletionEventMapper extends Mapper<LongWritable, Text, Text, EventsWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int ideSessionIdIndex = 0;
		int triggeredAtIndex = 1;
		int terminatedAtIndex = 2;
		int triggeredByIndex = 3;
		int eventClassIndex = 4;
		int extraInfoStartIndex = 5;

		String[] tokens = value.toString().split("\t");

		if (tokens.length < extraInfoStartIndex) {

			String message = "Not enough info on line (" + tokens.length + "/" + extraInfoStartIndex + ") : "
					+ value.toString();
			System.err.println(message);

		} else {

			String ideSessionId = tokens[ideSessionIdIndex];
			ZonedDateTime triggeredAt = ZonedDateTime.parse(tokens[triggeredAtIndex], Constants.DATE_TIME_FORMAT);
			ZonedDateTime terminatedAt = ZonedDateTime.parse(tokens[terminatedAtIndex], Constants.DATE_TIME_FORMAT);
			String triggeredBy = tokens[triggeredByIndex];
			String eventClass = tokens[eventClassIndex];
			String[] extraInfo = Arrays.copyOfRange(tokens, extraInfoStartIndex, tokens.length);

			if (eventClass.equalsIgnoreCase("CompletionEvent")) {

				EventsWritable eventsWritable = new EventsWritable(ideSessionId, triggeredAt, terminatedAt, triggeredBy,
						eventClass, extraInfo);

				context.write(new Text("key"), eventsWritable);
			}
		}

	}
}
