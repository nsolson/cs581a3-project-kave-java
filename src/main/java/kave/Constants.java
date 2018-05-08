package kave;

import java.io.File;
import java.time.format.DateTimeFormatter;

public class Constants {

	public static final String PROJECT_BASE_DIR = "D:\\Personal\\_MySVN_\\SVN\\current_branch\\Documents\\Grad_School\\cs581a3\\Assignments\\project";
	public static final String EVENTS_DIR = PROJECT_BASE_DIR + File.separator + "Events-170301-2";
	public static final String OUT_DIR = PROJECT_BASE_DIR + File.separator + "my_out";

	public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSZ");

}
