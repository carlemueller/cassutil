package helpers

class LogUtil {
    static void setLogLevel(String loggername, String lvl) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(loggername == null ? "ROOT" : loggername)
        logger.setLevel(ch.qos.logback.classic.Level.toLevel(lvl, ch.qos.logback.classic.Level.DEBUG))
    }

    static void setLogLevel(Class c, String lvl) {
        setLogLevel(c.package.name + "."+ c.name, lvl)
    }
}
