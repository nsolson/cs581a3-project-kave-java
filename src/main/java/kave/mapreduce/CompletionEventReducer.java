package kave.mapreduce;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompletionEventReducer extends Reducer<Text, EventsWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<EventsWritable> values, Context context)
			throws IOException, InterruptedException {

		long total = 0;
		long applied = 0;
		long cancelled = 0;
		long filtered = 0;
		long unknown = 0;
		long totalMillis = 0;
		long appliedMillis = 0;
		long cancelledMillis = 0;
		long filteredMillis = 0;
		long unknownMillis = 0;
		for (EventsWritable value : values) {

			String[] extraInfo = value.getExtraInfo();
			if (extraInfo.length == 4) {

				String terminatedState = extraInfo[1];

				long duration = ChronoUnit.MILLIS.between(value.getTriggeredAt(), value.getTerminatedAt());
				totalMillis += duration;
				++total;

				switch (terminatedState) {
				case "Applied":
					++applied;
					appliedMillis += duration;
					break;
				case "Cancelled":
					++cancelled;
					cancelledMillis += duration;
					break;
				case "Filtered":
					++filtered;
					filteredMillis += duration;
					break;
				default:
					++unknown;
					unknownMillis += duration;
				}
			}

		}

		// Total
		String totalStr = total + "\t" + ((double) total / (double) total) + "\t" + totalMillis + "\t"
				+ ((double) totalMillis / (double) totalMillis);
		context.write(new Text("Total"), new Text(totalStr));

		// Applied
		String appliedStr = applied + "\t" + ((double) applied / (double) total) + "\t" + appliedMillis + "\t"
				+ ((double) appliedMillis / (double) totalMillis);
		context.write(new Text("Applied"), new Text(appliedStr));

		// Cancelled
		String cancelledStr = cancelled + "\t" + ((double) cancelled / (double) total) + "\t" + cancelledMillis + "\t"
				+ ((double) cancelledMillis / (double) totalMillis);
		context.write(new Text("Cancelled"), new Text(cancelledStr));

		// Filtered
		String filteredStr = filtered + "\t" + ((double) filtered / (double) total) + "\t" + filteredMillis + "\t"
				+ ((double) filteredMillis / (double) totalMillis);
		context.write(new Text("Filtered"), new Text(filteredStr));

		// Unknown
		String unknownStr = unknown + "\t" + ((double) unknown / (double) total) + "\t" + unknownMillis + "\t"
				+ ((double) unknownMillis / (double) totalMillis);
		context.write(new Text("Unknown"), new Text(unknownStr));
	}

}
