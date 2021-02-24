import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.server.SimpleDB;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class CompareTime {
    private static SimpleDB db;
    private static int BUFFER_SIZE; // each test will run BUFFER_SIZE times every iteration
    private static int NUM_REPEATS; // how many time each test should be run
    private static long timer; // records the performance after BUFFER_SIZE * NUM_REPEATS iterations
    private static PrintWriter pw;

    private static boolean[] random; // true for pin, false for unpin

    // TODO: enter the name of the new buffer manager below
    private static final String BUFFERMGR_NEW = "BufferMgrBuffMap";

    private static final String SAVE_DIR = "project1out";
    private static final String TEST_DIR = "project1out/tests";

    public static void main(String[] args) {
        init(100, 100);
        runAll(100, 3000, 100);
    }

    public static void runAll(int start, int end, int step) {
        try {
            Method[] methods = Class.forName("CompareTime").getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().indexOf("_test") == 0) {
                    runRange(method.getName(), start, end, step);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void runRange(String test, int start, int end, int step) {
        System.out.println("generating file for: " + test);
        try {
            String filename = TEST_DIR + "/" + test + "-" + BUFFERMGR_NEW + ".txt";
            pw = new PrintWriter(new FileWriter(filename, false));
            for (int i = start; i <= end; i += step) {
                System.out.println(i);
                init(i, NUM_REPEATS);
                pw.printf("BUFFER_SIZE:%d\n", BUFFER_SIZE);
                long[] times = compareTest(test);
                pw.printf("%d,%d\n", times[0], times[1]);
            }
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long[] compareTest(String test) {
        generateRandom(BUFFER_SIZE);
        long original_time = -1, new_time = -1;
        try {
            original_time = (Long) Class.forName("CompareTime").getDeclaredMethod(test).invoke(null);
            db.setBufferMgr(BUFFERMGR_NEW);
            new_time = (Long) Class.forName("CompareTime").getDeclaredMethod(test).invoke(null);
        } catch (ClassNotFoundException |
                IllegalAccessException |
                InvocationTargetException |
                NoSuchMethodException e) {
            e.printStackTrace();
        }
        return new long[]{original_time, new_time};
    }

    // pins the same block over and over again to test for better block finding
    public static long _testPinSame() {
        BufferMgr bm = db.bufferMgr();
        startTimer();
        try {
            for (int i = 0; i < NUM_REPEATS; i++) {
                for (int j = 0; j < BUFFER_SIZE; j++) {
                    bm.pin(new BlockId("testfile", 0));
                }
            }
        } catch (BufferAbortException e) {
            return -1;
        }
        return recordTime();
    }

    // pins and unpins to test for replacement strategy
    public static long _testLoopRead() {
        BufferMgr bm = db.bufferMgr();
        startTimer();
        try {
            for (int i = 0; i < NUM_REPEATS; i++) {
                Buffer[] buffs = new Buffer[BUFFER_SIZE];
                for (int j = 0; j < BUFFER_SIZE; j++) {
                    buffs[j] = bm.pin(new BlockId("testfile", j));
                }
                for (int j = 0; j < BUFFER_SIZE; j++) {
                    bm.unpin(buffs[j]);
                }
            }
        } catch (BufferAbortException e) {
            return -1;
        }
        return recordTime();
    }

    // random pins and unpins
    public static long _testRandom() {
        BufferMgr bm = db.bufferMgr();
        List<Buffer> buffs = new ArrayList<>();
        startTimer();
        try {
            for (int i = 0; i < NUM_REPEATS; i++) {
                for (int j = 0; j < random.length; j++) {
                    if (random[j] || buffs.size() == 0) {
                        buffs.add(bm.pin(new BlockId("testfile", j)));
                    } else {
                        bm.unpin(buffs.remove(0));
                    }
                }
            }
        } catch (BufferAbortException e) {
            return -1;
        }
        return recordTime();
    }

    public static void generateRandom(int length) {
        random = new boolean[length];
        for (int i = 0; i < length; i++) {
            random[i] = (int) (Math.random() * 2) == 1;
        }
    }

    public static void startTimer() {
        timer = System.currentTimeMillis();
    }

    public static long recordTime() {
        return System.currentTimeMillis() - timer;
    }

    public static void init(int bufferSize, int numRepeats) {
        BUFFER_SIZE = bufferSize;
        NUM_REPEATS = numRepeats;
        db = new SimpleDB("project1out", 400, BUFFER_SIZE); // only 3 buffers
        File saveDir = new File(TEST_DIR);
        if (!saveDir.exists()) {
            saveDir.mkdirs();
        }
    }
}
