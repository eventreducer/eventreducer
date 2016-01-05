package org.eventreducer.annotations;

import org.eventreducer.IndexFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
public @interface Index {
    IndexFactory.IndexFeature[] features() default {IndexFactory.IndexFeature.EQ};
}
