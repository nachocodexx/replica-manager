package mx.cinvestav.log;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.LayoutBase;

import java.util.Arrays;

public class MySampleLayout extends LayoutBase<ILoggingEvent> {

        String nodeId = "sn";
        public void setNodeId(String nodeId) {
            this.nodeId=nodeId;
        }

        public String doLayout(ILoggingEvent event) {
//            event.getLo
            StringBuffer sbuf = new StringBuffer(128);
//            event.getLoggerContextVO().getBirthTime()
            long elapsedTime = event.getTimeStamp() - event.getLoggerContextVO().getBirthTime();
            String threadName = event.getThreadName();
            String level = event.getLevel().toString();
            String loggerName = event.getLoggerName();
            String message = event.getFormattedMessage();
//            sbuf.append(elapsedTime);
            sbuf
                    .append(elapsedTime)
                    .append(" ")
                    .append(message)
                    .append(" ")
                    .append(CoreConstants.LINE_SEPARATOR);
//  dDEBUG
//            sbuf.append(event.getTimeStamp())
//                    .append(" ")
//                    .append(nodeId).append(" ").append(elapsedTime)
//                    .append(" ")
//                    .append(level)
//                    .append(" [")
//                    .append(threadName)
//                    .append("]")
//                    .append(" ")
//                    .append(message)
//                    .append(CoreConstants.LINE_SEPARATOR);
            return sbuf.toString();
        }
}
