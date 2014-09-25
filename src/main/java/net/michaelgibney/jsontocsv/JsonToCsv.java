package net.michaelgibney.jsontocsv;

import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Several design choices that were not in the specs are specified here.
 * The parser is tolerant.  Absence of fields or invalid records are tolerated
 * and logged at level WARN. 
 * No attempt is made to validate the field contents.  All fields are parsed as
 * Strings.  It would be trivial to introduce such validation, but the program 
 * as currently implemented is strictly a transformer, not a validator. 
 * Most other types of errors (IOExceptions, problems opening streams, Json parsing
 * errors, are treated as unrecoverable and fall to the UncaughtExceptionHandler,
 * ending the program. 
 * The input string is taken directly and escaped to exclude characters that are
 * illegal in the URI path element.  Forward slashes in the input are left untouched.
 *
 */
public class JsonToCsv implements Runnable {

    /**
     * A character class for specifying characters the presence of which will cause
     * a field to be enclosed in double-quotes. All but comma could potentially 
     * be optional, depending on desired output csv format.
     */
    private static final Pattern SPECIAL_CHARACTERS = Pattern.compile("[,\"\n\r]");
    
    /**
     * Special handling of the replacement string requires double-escaping of 
     * backslashes. Some csv formats call for double-double-quote to escape quotes
     * in field values; that configuration would be changed here.
     */
    private static final String DOUBLE_QUOTE_REPLACEMENT = "\\\\\"";
    
    private static enum OutputFields { _id, name, type, latitude, longitude };
    private static enum OtherFields { record, geo_position };
    private static final int FIELD_COUNT = OutputFields.values().length; // convenience
    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
    private static final Pattern PARSE_CHARSET = Pattern.compile("[ ;]charset=([^ ;]+)");
    private static final Logger LOG = LoggerFactory.getLogger(JsonToCsv.class);
    
    private final String query;
    private PrintStream out = System.out;
    private final Map<OutputFields,String> rowMap = new EnumMap<>(OutputFields.class);
    
    /**
     * Cache and reuse StringBuilder and StringBuffer
     */
    private final StringBuilder sb = new StringBuilder();
    private final StringBuffer sbuf = new StringBuffer();

    private void reset() {
        deferredLog.clear();
        rowMap.clear();
        sb.setLength(0);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        JsonToCsv app;
        if (args == null || args.length < 1) {
            Reader r = new InputStreamReader(System.in);
            app = new JsonToCsv(null);
            app.parseStream(r);
        } else {
            app = new JsonToCsv(args[0]);
            app.run();
        }
    }
    
    private static URI getURI(String input) throws URISyntaxException {
        URIBuilder b = new URIBuilder();
        b.setScheme("http");
        b.setHost("api.goeuro.com");
        b.setPath("/api/v2/position/suggest/en/".concat(input));
        return b.build();
    }
    
    public JsonToCsv(String query) {
        this.query = query;
    }
    
    public void setOut(PrintStream out) {
        this.out = out;
    }
    
    @Override
    public void run() {
        try {
            URI uri = getURI(query);
            URLConnection conn = uri.toURL().openConnection();
            String contentType = conn.getContentType();
            Matcher m = PARSE_CHARSET.matcher(contentType);
            String charset;
            if (!m.find()) {
                charset = null;
            } else {
                charset = m.group(1);
            }
            InputStream in = conn.getInputStream();
            try {
                Reader r;
                if (charset == null) {
                    r = new InputStreamReader(in);
                } else {
                    r = new InputStreamReader(in, charset);
                }
                parseStream(r);
            } finally {
                in.close();
            }
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void parseStream(Reader r) throws IOException {
        JsonReader jr = new JsonReader(r);
        jr.setLenient(true);
        parseJson(jr);
    }
    
    public void parseJson(JsonReader r) throws IOException {
        r.beginArray();
        while (r.hasNext()) {
            reset();
            handleJsonFieldAsObject(OtherFields.record, r);
            writeRow();
        }
        r.endArray();
        r.close();
    }
    
    private void parseRecord(JsonReader r) throws IOException {
        while (r.hasNext()) {
            String key = r.nextName();
            if ("_id".equals(key)) {
                handleJsonFieldAsString(OutputFields._id, r);
            } else if ("name".equals(key)) {
                handleJsonFieldAsString(OutputFields.name, r);
            } else if ("type".equals(key)) {
                handleJsonFieldAsString(OutputFields.type, r);
            } else if ("geo_position".equals(key)) {
                handleJsonFieldAsObject(OtherFields.geo_position, r);
            } else {
                r.skipValue();
            }
        }
    }
    
    private void handleJsonFieldAsString(OutputFields type, JsonReader r) throws IOException {
        try {
            rowMap.put(type, r.nextString());
        } catch (IllegalStateException ex) {
            deferredLog.add(new DeferredLogEntry(LogLevel.warn, "field "+type+" has type "+
                    r.peek().toString()+"; unable to be read as String"));
            r.skipValue();
        }
    }

    private void handleJsonFieldAsObject(OtherFields type, JsonReader r) throws IOException {
        try {
            r.beginObject();
        } catch (IllegalStateException ex) {
            deferredLog.add(new DeferredLogEntry(LogLevel.warn, "field "+type+" has type "+
                    r.peek().toString()+"; unable to be read as Object"));
            r.skipValue();
            return;
        }
        switch (type) {
            case record:
                parseRecord(r);
                break;
            case geo_position:
                parsePosition(r);
                break;
            default:
                throw new RuntimeException("Handling not defined for field \""+type+'"');

        }
        r.endObject();
    }

    private void parsePosition(JsonReader r) throws IOException {
        while (r.hasNext()) {
            String key = r.nextName();
            if ("latitude".equals(key)) {
                handleJsonFieldAsString(OutputFields.latitude, r);
            } else if ("longitude".equals(key)) {
                handleJsonFieldAsString(OutputFields.longitude, r);
            } else {
                r.skipValue();
            }
        }
    }

    private void writeRow() {
        if (!deferredLog.isEmpty()) {
            writeDeferredLogMessages();
        }
        if (rowMap.size() != FIELD_COUNT) {
            Set<OutputFields> missing = EnumSet.allOf(OutputFields.class);
            missing.removeAll(rowMap.keySet());
            LOG.warn("required fields not found: " + missing + " for _id="+rowMap.get(OutputFields._id));
        }
        OutputFields[] keys = OutputFields.values();
        writeField(rowMap.get(keys[0]), sb);
        for (int i = 1; i < keys.length; i++) {
            sb.append(",");
            writeField(rowMap.get(keys[i]), sb);
        }
        out.println(sb.toString());
    }
    
    private void writeField(CharSequence cs, StringBuilder sb) {
        if (cs == null) {
            return; // in output will be indistinguishable from empty String
        } else if (!SPECIAL_CHARACTERS.matcher(cs).find()) {
            sb.append(cs); // no special handling required
        } else {
            // enclose field in double-quotes
            Matcher m = DOUBLE_QUOTE.matcher(cs);
            sb.append('"');
            if (!m.find()) {
                sb.append(cs); // field contents need not be escaped
            } else {
                // must escape double-quotes within quoted field
                sbuf.setLength(0);
                do {
                    m.appendReplacement(sbuf, DOUBLE_QUOTE_REPLACEMENT);
                } while (m.find());
                m.appendTail(sbuf);
                sb.append(sbuf);
            }
            sb.append('"');
        }
    }

    /**
     * We are deferring log messages to ensure that <code>_id</code> is accessible
     * for more meaningful log messages.
     */
    private final ArrayList<DeferredLogEntry> deferredLog = new ArrayList<>();

    private static enum LogLevel { trace, debug, info, warn, error };
    
    private void writeDeferredLogMessages() {
        for (DeferredLogEntry e : deferredLog) {
            String message = e.message.concat(" for _id=" + rowMap.get(OutputFields._id));
            switch (e.level) {
                case trace:
                    LOG.trace(message, e.t);
                    break;
                case debug:
                    LOG.debug(message, e.t);
                    break;
                case info:
                    LOG.info(message, e.t);
                    break;
                case warn:
                    LOG.warn(message, e.t);
                    break;
                case error:
                    LOG.error(message, e.t);
                    break;
                default:
                    throw new RuntimeException("unknown " + LogLevel.class.getSimpleName() + ": " + e.level);
            }
        }
    }
    
    private static class DeferredLogEntry {
        private final LogLevel level;
        private final String message;
        private final Throwable t;
        private DeferredLogEntry(LogLevel level, String message) {
            this(level, message, null);
        }
        private DeferredLogEntry(LogLevel level, String message, Throwable t) {
            this.level = level;
            this.message = message;
            this.t = t;
        }
    }
    
}
