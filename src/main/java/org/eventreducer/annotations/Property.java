package org.eventreducer.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies inclusion of a field as a property of a Command or a Message.
 * Target field should not be private or protected as it has to be accessible
 * to the compile-time generated Serializer class. At the very least, it has to
 * be package-local.
 *
 * Example:
 *
 * <code>
 *     public class MyEvent extends Event {
 *         &#64;Property
 *         int value;
 *     }
 * </code>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Property {
}
