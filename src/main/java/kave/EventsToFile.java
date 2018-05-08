package kave;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Set;

import cc.kave.commons.model.events.ActivityEvent;
import cc.kave.commons.model.events.CommandEvent;
import cc.kave.commons.model.events.ErrorEvent;
import cc.kave.commons.model.events.EventTrigger;
import cc.kave.commons.model.events.IDEEvent;
import cc.kave.commons.model.events.InfoEvent;
import cc.kave.commons.model.events.NavigationEvent;
import cc.kave.commons.model.events.SystemEvent;
import cc.kave.commons.model.events.completionevents.CompletionEvent;
import cc.kave.commons.model.events.tasks.TaskEvent;
import cc.kave.commons.model.events.testrunevents.TestCaseResult;
import cc.kave.commons.model.events.testrunevents.TestRunEvent;
import cc.kave.commons.model.events.userprofiles.UserProfileEvent;
import cc.kave.commons.model.events.versioncontrolevents.VersionControlAction;
import cc.kave.commons.model.events.versioncontrolevents.VersionControlEvent;
import cc.kave.commons.model.events.visualstudio.BuildEvent;
import cc.kave.commons.model.events.visualstudio.DebuggerEvent;
import cc.kave.commons.model.events.visualstudio.DocumentEvent;
import cc.kave.commons.model.events.visualstudio.EditEvent;
import cc.kave.commons.model.events.visualstudio.FindEvent;
import cc.kave.commons.model.events.visualstudio.IDEStateEvent;
import cc.kave.commons.model.events.visualstudio.InstallEvent;
import cc.kave.commons.model.events.visualstudio.SolutionEvent;
import cc.kave.commons.model.events.visualstudio.UpdateEvent;
import cc.kave.commons.model.events.visualstudio.WindowEvent;
import cc.kave.commons.utils.io.IReadingArchive;
import cc.kave.commons.utils.io.ReadingArchive;
import examples.IoHelper;

public class EventsToFile {

	public static void main(String[] args) {

		Set<String> zips = IoHelper.findAllZips(Constants.EVENTS_DIR);

		String outFile = Constants.OUT_DIR + File.separator + "all_events.txt";
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(outFile);

			int totalZips = zips.size();
			int zipCount = 0;
			for (String zip : zips) {
				++zipCount;

				File zipFile = Paths.get(Constants.EVENTS_DIR, zip).toFile();
				System.out.println(zip);

				int printCounter = 0;
				IReadingArchive ra = null;
				try {
					ra = new ReadingArchive(zipFile);
					int archiveSize = ra.getNumberOfEntries();
					while (ra.hasNext()) {
						if ((++printCounter) % 1000 == 0) {
							System.out.println(
									zip + " (" + zipCount + "/" + totalZips + "): " + printCounter + "/" + archiveSize);
						}

						IDEEvent event = ra.getNext(IDEEvent.class);
						String sessionId = event.IDESessionUUID;
						ZonedDateTime triggeredAt = event.getTriggeredAt();
						ZonedDateTime terminatedAt = null;
						try {
							terminatedAt = event.getTerminatedAt();
						} catch (Exception e) {
							terminatedAt = triggeredAt;
						}
						EventTrigger triggeredBy = event.TriggeredBy;

						try {
							StringBuilder sb = new StringBuilder();
							String triggeredAtStr = triggeredAt.format(Constants.DATE_TIME_FORMAT);
							String terminatedAtStr = terminatedAt.format(Constants.DATE_TIME_FORMAT);
							sb.append(sessionId).append("\t").append(triggeredAtStr).append("\t")
									.append(terminatedAtStr).append("\t").append(triggeredBy).append("\t")
									.append(event.getClass().getSimpleName());

							if (event instanceof ActivityEvent) {

							} else if (event instanceof BuildEvent) {

								BuildEvent buildEvent = (BuildEvent) event;
								sb.append("\t").append(buildEvent.Scope).append("\t").append(buildEvent.Action);

							} else if (event instanceof CommandEvent) {

								CommandEvent commandEvent = (CommandEvent) event;
								sb.append("\t").append(commandEvent.getCommandId());

							} else if (event instanceof CompletionEvent) {

								CompletionEvent completionEvent = (CompletionEvent) event;
								sb.append("\t").append(completionEvent.terminatedBy).append("\t")
										.append(completionEvent.terminatedState).append("\t")
										.append(completionEvent.getProposalCount()).append("\t")
										.append(completionEvent.getSelections().size());

							} else if (event instanceof DebuggerEvent) {

								DebuggerEvent debuggerEvent = (DebuggerEvent) event;
								sb.append("\t").append(debuggerEvent.Mode).append("\t").append(debuggerEvent.Reason)
										.append("\t" + debuggerEvent.Action);

							} else if (event instanceof DocumentEvent) {

								DocumentEvent documentEvent = (DocumentEvent) event;
								sb.append("\t").append(documentEvent.Document.getFileName()).append("\t")
										.append(documentEvent.Action);

							} else if (event instanceof EditEvent) {

								EditEvent editEvent = (EditEvent) event;
								sb.append("\t").append(editEvent.NumberOfChanges).append("\t")
										.append(editEvent.SizeOfChanges);

							} else if (event instanceof ErrorEvent) {

								ErrorEvent errorEvent = (ErrorEvent) event;
								sb.append("\t").append(errorEvent.Content);
								for (String stackTrace : errorEvent.StackTrace) {
									sb.append("\t").append(stackTrace);
								}

							} else if (event instanceof FindEvent) {

								FindEvent findEvent = (FindEvent) event;
								sb.append("\t").append(findEvent.Cancelled);

							} else if (event instanceof IDEStateEvent) {

								IDEStateEvent ideStateEvent = (IDEStateEvent) event;
								sb.append("\t").append(ideStateEvent.IDELifecyclePhase).append("\t")
										.append(ideStateEvent.OpenWindows.size() + "\t")
										.append(ideStateEvent.OpenDocuments.size());

							} else if (event instanceof InfoEvent) {

								InfoEvent infoEvent = (InfoEvent) event;
								sb.append("\t").append(infoEvent.Info);

							} else if (event instanceof InstallEvent) {

								InstallEvent installEvent = (InstallEvent) event;
								sb.append("\t").append(installEvent.PluginVersion);

							} else if (event instanceof NavigationEvent) {

								NavigationEvent navigationEvent = (NavigationEvent) event;
								sb.append("\t").append(navigationEvent.TypeOfNavigation)
										.append("\t" + navigationEvent.Target.getIdentifier())
										.append("\t" + navigationEvent.Location.getIdentifier());

							} else if (event instanceof SolutionEvent) {

								SolutionEvent solutionEvent = (SolutionEvent) event;
								sb.append("\t").append(solutionEvent.Action).append("\t")
										.append(solutionEvent.Target.getIdentifier());

							} else if (event instanceof SystemEvent) {

								SystemEvent systemEvent = (SystemEvent) event;
								sb.append("\t").append(systemEvent.Type);

							} else if (event instanceof TaskEvent) {

								TaskEvent taskEvent = (TaskEvent) event;
								sb.append("\t").append(taskEvent.getVersion()).append("\t")
										.append(taskEvent.getTaskId()).append("\t" + taskEvent.getAction()).append("\t")
										.append(taskEvent.getNewParentId()).append("\t" + taskEvent.getAnnoyance())
										.append("\t").append(taskEvent.getImportance())
										.append("\t" + taskEvent.getUrgency());

							} else if (event instanceof TestRunEvent) {

								TestRunEvent testRunEvent = (TestRunEvent) event;
								sb.append("\t").append(testRunEvent.WasAborted);
								for (TestCaseResult testCaseResult : testRunEvent.Tests) {
									sb.append("\t").append(testCaseResult.Result);
								}

							} else if (event instanceof UpdateEvent) {

								UpdateEvent updateEvent = (UpdateEvent) event;
								sb.append("\t").append(updateEvent.OldPluginVersion).append("\t")
										.append(updateEvent.NewPluginVersion);

							} else if (event instanceof UserProfileEvent) {

								UserProfileEvent userProfileEvent = (UserProfileEvent) event;
								sb.append("\t").append(userProfileEvent.ProfileId);

							} else if (event instanceof VersionControlEvent) {

								VersionControlEvent versionControlEvent = (VersionControlEvent) event;
								sb.append("\t").append(versionControlEvent.Solution.getIdentifier());
								for (VersionControlAction action : versionControlEvent.Actions) {
									sb.append("\t").append(action.ActionType);
								}

							} else if (event instanceof WindowEvent) {

								WindowEvent windowEvent = (WindowEvent) event;
								sb.append("\t").append(windowEvent.Window.getIdentifier()).append("\t")
										.append(windowEvent.Action);
							}

							pw.println(sb.toString());

						} catch (Exception e) {
							// Skip this line
						}
					}

				} finally {
					if (ra != null) {
						ra.close();
					}
				}
			}

		} catch (

		FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}

}
