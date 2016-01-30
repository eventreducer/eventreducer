package org.eventreducer;

import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.hlc.HybridTimestamp;

public class ComparableTimeStamp implements Comparable<ComparableTimeStamp> {

    private final TimeStamp timeStamp;

    public ComparableTimeStamp(TimeStamp timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public int compareTo(ComparableTimeStamp anotherTimeStamp) {
        return HybridTimestamp.compare(this.timeStamp, anotherTimeStamp.timeStamp);
    }
}
