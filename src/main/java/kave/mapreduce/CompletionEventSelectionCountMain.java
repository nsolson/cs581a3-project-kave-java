package kave.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CompletionEventSelectionCountMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.err.println("CORRECT USAGE --> Usage: <jar file> <input file> <out dir>");
			System.exit(-1);
		}

		String inFile = args[0];
		String outDir = args[1];

		Configuration confOne = new Configuration();
		Job jobOne = Job.getInstance(confOne);
		jobOne.setJarByClass(CompletionEventSelectionCountMain.class);
		jobOne.setMapperClass(CompletionEventMapper.class);
		jobOne.setReducerClass(CompletionEventSelectionCountReducer.class);
		jobOne.setMapOutputKeyClass(Text.class);
		jobOne.setMapOutputValueClass(EventsWritable.class);
		jobOne.setOutputKeyClass(Text.class);
		jobOne.setOutputValueClass(Text.class);
		jobOne.setInputFormatClass(TextInputFormat.class);
		jobOne.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobOne, new Path(inFile));
		FileOutputFormat.setOutputPath(jobOne, new Path(outDir));

		if (jobOne.waitForCompletion(true)) {
			System.out.println("Finished");
		} else {
			System.err.println("Failed to finish!");
		}
	}

}
