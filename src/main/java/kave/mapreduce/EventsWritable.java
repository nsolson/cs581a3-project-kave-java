package kave.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZonedDateTime;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import kave.Constants;

public class EventsWritable implements WritableComparable<EventsWritable> {

	/**
	 * The IDE session ID of the event
	 */
	private String ideSessionId;

	/**
	 * The time the event was triggered at
	 */
	private ZonedDateTime triggeredAt;

	/**
	 * The time the event was terminated at
	 */
	private ZonedDateTime terminatedAt;

	/**
	 * How the event was triggered
	 */
	private String triggeredBy;

	/**
	 * The specific event class
	 */
	private String eventClass;

	/**
	 * Extra information held by some event classes
	 */
	private String[] extraInfo;

	/**
	 * Default constructor required
	 */
	public EventsWritable() {
	}

	public EventsWritable(String ideSessionId, String triggeredAt, String terminatedAt, String triggeredBy,
			String eventClass, String... extraInfo) {

		this(ideSessionId, ZonedDateTime.parse(triggeredAt, Constants.DATE_TIME_FORMAT),
				ZonedDateTime.parse(terminatedAt, Constants.DATE_TIME_FORMAT), triggeredBy, eventClass, extraInfo);
	}

	public EventsWritable(String ideSessionId, ZonedDateTime triggeredAt, ZonedDateTime terminatedAt,
			String triggeredBy, String eventClass, String... extraInfo) {

		this.ideSessionId = ideSessionId;
		this.triggeredAt = triggeredAt;
		this.terminatedAt = terminatedAt;
		this.triggeredBy = triggeredBy;
		this.eventClass = eventClass;
		this.extraInfo = extraInfo;
	}

	public String getIdeSessionId() {
		return ideSessionId;
	}

	public ZonedDateTime getTriggeredAt() {
		return triggeredAt;
	}

	public ZonedDateTime getTerminatedAt() {
		return terminatedAt;
	}

	public String getTriggeredBy() {
		return triggeredBy;
	}

	public String getEventClass() {
		return eventClass;
	}

	public String[] getExtraInfo() {
		return extraInfo;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		ideSessionId = WritableUtils.readString(in);
		triggeredAt = ZonedDateTime.parse(WritableUtils.readString(in), Constants.DATE_TIME_FORMAT);
		terminatedAt = ZonedDateTime.parse(WritableUtils.readString(in), Constants.DATE_TIME_FORMAT);
		triggeredBy = WritableUtils.readString(in);
		eventClass = WritableUtils.readString(in);
		extraInfo = WritableUtils.readString(in).split("\t");
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, ideSessionId);
		WritableUtils.writeString(out, triggeredAt.format(Constants.DATE_TIME_FORMAT));
		WritableUtils.writeString(out, terminatedAt.format(Constants.DATE_TIME_FORMAT));
		WritableUtils.writeString(out, triggeredBy);
		WritableUtils.writeString(out, eventClass);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < extraInfo.length; i++) {
			sb.append(extraInfo[i]);
			if ((i + 1) < extraInfo.length) {
				sb.append("\t");
			}
		}

		WritableUtils.writeString(out, sb.toString());
	}

	/**
	 * Sorts in the following order:
	 * <ol>
	 * <li>ideSessionId</li>
	 * <li>triggeredAt</li>
	 * <li>terminatedAt</li>
	 * <li>eventClass</li>
	 * <li>triggeredBy</li>
	 * </ol>
	 * 
	 * @param other
	 *            The other {@link EventsWritable} to compare to
	 * @return -1 if this item should go first, 1 if the other item should go first,
	 *         0 if they are equal
	 */
	@Override
	public int compareTo(EventsWritable other) {

		if (other == null) {
			return -1;
		}

		if (!ideSessionId.equalsIgnoreCase(other.getIdeSessionId())) {
			return ideSessionId.compareTo(other.getIdeSessionId());
		} else if (triggeredAt.isBefore(other.getTriggeredAt())) {
			return -1;
		} else if (triggeredAt.isAfter(other.getTriggeredAt())) {
			return 1;
		} else if (terminatedAt.isBefore(other.getTerminatedAt())) {
			return -1;
		} else if (terminatedAt.isAfter(other.getTerminatedAt())) {
			return 1;
		} else if (!eventClass.equalsIgnoreCase(other.getEventClass())) {
			return eventClass.compareTo(other.getEventClass());
		} else if (!triggeredBy.equalsIgnoreCase(other.getTriggeredBy())) {
			return triggeredBy.compareTo(other.getTriggeredBy());
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object o) {

		if (o == null) {
			return false;
		}

		if (o instanceof EventsWritable) {

			EventsWritable other = (EventsWritable) o;
			return this.compareTo(other) == 0;

		} else {
			return false;
		}
	}

	@Override
	public String toString() {

		String triggeredAtStr = triggeredAt.format(Constants.DATE_TIME_FORMAT);
		String terminatedAtStr = terminatedAt.format(Constants.DATE_TIME_FORMAT);

		StringBuilder sb = new StringBuilder();
		sb.append(ideSessionId);
		sb.append("\t").append(triggeredAtStr);
		sb.append("\t").append(terminatedAtStr);
		sb.append("\t").append(triggeredBy);
		sb.append("\t").append(eventClass);

		for (String info : extraInfo) {
			sb.append("\t").append(info);
		}

		return sb.toString();
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
}
