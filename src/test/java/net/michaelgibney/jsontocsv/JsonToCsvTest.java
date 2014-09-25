package net.michaelgibney.jsontocsv;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Given the overhead of mocking JsonReaders, it makes sense to write tests as 
 * functional tests rather than true unit tests. Several special cases and associated
 * input are tested below.  
 * 
 * @author magibney
 */
public class JsonToCsvTest {

    public JsonToCsvTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class App.
     */
    //@Test
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = null;
        JsonToCsv.main(args);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    private static class ComparingOutputStream extends OutputStream {

        private final InputStream compare;

        private ComparingOutputStream(InputStream compare) {
            this.compare = compare;
        }

        @Override
        public void write(int b) throws IOException {
            assertEquals(b, compare.read());
        }

        @Override
        public void close() throws IOException {
            super.close();
            assertEquals(-1, compare.read());
            compare.close();
        }

    }

    @Test
    public void testNormal() throws Exception {
        runTestIndex(1);
    }

    @Test
    public void testInvalidRecordObject() throws Exception {
        runTestIndex(2);
    }

    @Test
    public void testInvalidFieldObject() throws Exception {
        runTestIndex(3);
    }

    @Test
    public void testMissingId() throws Exception {
        runTestIndex(4);
    }

    @Test
    public void testFieldWithComma() throws Exception {
        runTestIndex(5);
    }

    @Test
    public void testFieldWithCommaAndQuotes() throws Exception {
        runTestIndex(6);
    }

    @Test
    public void testFieldWithQuotes() throws Exception {
        runTestIndex(7);
    }

    private void runTestIndex(int index) throws UnsupportedEncodingException, IOException {
        JsonToCsv app = new JsonToCsv(null);
        OutputStream compare = new ComparingOutputStream(ClassLoader.getSystemResourceAsStream("output" + index + ".csv"));
        PrintStream ps = new PrintStream(compare, true, "UTF-8");
        app.setOut(ps);
        app.parseStream(new InputStreamReader(ClassLoader.getSystemResourceAsStream("input" + index + ".json")));
        ps.close();
    }

}
