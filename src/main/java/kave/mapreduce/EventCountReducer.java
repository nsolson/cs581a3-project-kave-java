package kave.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventCountReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		long total = 0;
		long activity = 0;
		long build = 0;
		long command = 0;
		long completion = 0;
		long debugger = 0;
		long document = 0;
		long edit = 0;
		long error = 0;
		long find = 0;
		long ideState = 0;
		long info = 0;
		long install = 0;
		long navigation = 0;
		long solution = 0;
		long system = 0;
		long task = 0;
		long testRun = 0;
		long update = 0;
		long userProfile = 0;
		long versionControl = 0;
		long window = 0;
		long unknown = 0;

		for (Text eventText : values) {

			String event = eventText.toString();
			++total;
			if (event.equalsIgnoreCase("ActivityEvent")) {

				++activity;

			} else if (event.equalsIgnoreCase("BuildEvent")) {

				++build;

			} else if (event.equalsIgnoreCase("CommandEvent")) {

				++command;

			} else if (event.equalsIgnoreCase("CompletionEvent")) {

				++completion;

			} else if (event.equalsIgnoreCase("DebuggerEvent")) {

				++debugger;

			} else if (event.equalsIgnoreCase("DocumentEvent")) {

				++document;

			} else if (event.equalsIgnoreCase("EditEvent")) {

				++edit;

			} else if (event.equalsIgnoreCase("ErrorEvent")) {

				++error;

			} else if (event.equalsIgnoreCase("FindEvent")) {

				++find;

			} else if (event.equalsIgnoreCase("IDEStateEvent")) {

				++ideState;

			} else if (event.equalsIgnoreCase("InfoEvent")) {

				++info;

			} else if (event.equalsIgnoreCase("InstallEvent")) {

				++install;

			} else if (event.equalsIgnoreCase("NavigationEvent")) {

				++navigation;

			} else if (event.equalsIgnoreCase("SolutionEvent")) {

				++solution;

			} else if (event.equalsIgnoreCase("SystemEvent")) {

				++system;

			} else if (event.equalsIgnoreCase("TaskEvent")) {

				++task;

			} else if (event.equalsIgnoreCase("TestRunEvent")) {

				++testRun;

			} else if (event.equalsIgnoreCase("UpdateEvent")) {

				++update;

			} else if (event.equalsIgnoreCase("UserProfileEvent")) {

				++userProfile;

			} else if (event.equalsIgnoreCase("VersionControlEvent")) {

				++versionControl;

			} else if (event.equalsIgnoreCase("WindowEvent")) {

				++window;

			} else {

				++unknown;
			}
		}

		double dTotal = (double) total;
		context.write(new Text("Total"), new Text(total + "\t" + (dTotal / dTotal)));
		context.write(new Text("Activity"), new Text(activity + "\t" + ((double) activity / dTotal)));
		context.write(new Text("Build"), new Text(build + "\t" + ((double) build / dTotal)));
		context.write(new Text("Command"), new Text(command + "\t" + ((double) command / dTotal)));
		context.write(new Text("Completion"), new Text(completion + "\t" + ((double) completion / dTotal)));
		context.write(new Text("Debugger"), new Text(debugger + "\t" + ((double) debugger / dTotal)));
		context.write(new Text("Document"), new Text(document + "\t" + ((double) document / dTotal)));
		context.write(new Text("Edit"), new Text(edit + "\t" + ((double) edit / dTotal)));
		context.write(new Text("Error"), new Text(error + "\t" + ((double) error / dTotal)));
		context.write(new Text("Find"), new Text(find + "\t" + ((double) find / dTotal)));
		context.write(new Text("IdeState"), new Text(ideState + "\t" + ((double) ideState / dTotal)));
		context.write(new Text("Info"), new Text(info + "\t" + ((double) info / dTotal)));
		context.write(new Text("Install"), new Text(install + "\t" + ((double) install / dTotal)));
		context.write(new Text("Navigation"), new Text(navigation + "\t" + ((double) navigation / dTotal)));
		context.write(new Text("Solution"), new Text(solution + "\t" + ((double) solution / dTotal)));
		context.write(new Text("System"), new Text(system + "\t" + ((double) system / dTotal)));
		context.write(new Text("Task"), new Text(task + "\t" + ((double) task / dTotal)));
		context.write(new Text("TestRun"), new Text(testRun + "\t" + ((double) testRun / dTotal)));
		context.write(new Text("Update"), new Text(update + "\t" + ((double) update / dTotal)));
		context.write(new Text("UserProfile"), new Text(userProfile + "\t" + ((double) userProfile / dTotal)));
		context.write(new Text("VersionControl"), new Text(versionControl + "\t" + ((double) versionControl / dTotal)));
		context.write(new Text("Window"), new Text(window + "\t" + ((double) window / dTotal)));
		context.write(new Text("Unknown"), new Text(unknown + "\t" + ((double) unknown / dTotal)));
	}
}
