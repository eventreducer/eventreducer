package org.eventreducer;

import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.hlc.HybridTimestamp;

public class ComparableTimeStamp extends TimeStamp {

    public ComparableTimeStamp(TimeStamp timeStamp) {
        super(timeStamp.toString());
    }

    @Override
    public int compareTo(TimeStamp anotherTimeStamp) {
        return HybridTimestamp.compare(this, anotherTimeStamp);
    }
}
