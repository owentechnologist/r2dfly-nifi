package io.dragonfly.nifi.redis.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

/** The attribute every FlowFile routed to failure carries, so its dead-letter record says why. */
public final class FailureReason {

    public static final String ATTRIBUTE = "redis.failure.reason";

    private FailureReason() {
    }

    public static FlowFile tag(ProcessSession session, FlowFile flowFile, String reason) {
        return session.putAttribute(flowFile, ATTRIBUTE, reason);
    }

    public static FlowFile tag(ProcessSession session, FlowFile flowFile, String what, Throwable cause) {
        return tag(session, flowFile, what + ": " + describe(cause));
    }

    /** The innermost cause's message: the wrapping exceptions add only their class names. */
    public static String describe(Throwable cause) {
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null || message.isBlank() ? root.getClass().getSimpleName() : message;
    }
}
