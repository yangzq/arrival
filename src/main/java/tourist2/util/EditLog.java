package tourist2.util;

import java.io.*;

/**
 * 逐条记录收到的signal，支持从最后的的记录逐条读出.
 * 大约每秒写20万条记录
 */
public class EditLog {
//    private final File logDir;
    private File file;
    private DataOutputStream out;
    private int writeIndex = 0;
    private boolean isClosed = false;

    private RandomAccessFile readFile;
    private DataInputStream in;
    private long readPosition = 0;
    private int readIndex = 0;

    private long logIndex = 0;


    public EditLog(String imsi) throws IOException {
//        logDir = File.createTempFile("storm", imsi);
//        logDir.delete();
//        logDir.mkdir();
//        System.out.println(logDir.getCanonicalPath());
//        file = new File(logDir, writeIndex + "");
//        out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
//        logIndex = 0;
    }

    public void append(long time, boolean isInSide, long lastTime, boolean lastInside, long[] recentDays) throws IOException {
//
//        if (isClosed) {
//            file = new File(logDir, writeIndex + "");
//            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
//            logIndex = 0;
//            isClosed = false;
//        }
//        long size = out.size();
////    if (size > 1024 * 1024 * 512) { //大约每512M，换一个文件
//        if (size > 1024 * 1024 * 1) { //大约每1M，换一个文件
//            out.close();
//            file = new File(logDir, ++writeIndex + "");
//            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
//            logIndex = 0;
//            size = out.size();
//        }
//        out.writeLong(time);
//        out.writeBoolean(isInSide);
//        //如果每次都记录状态，则会影响速度，所以每隔100条记录，记录一下状态
//        boolean logStatus = ++logIndex % 100 == 1;
//        out.writeBoolean(logStatus);
//        if (logStatus) {
//            out.writeLong(lastTime);
//            out.writeBoolean(lastInside);
//            for (int i = 0; i < 10; i++) {
//                out.writeLong(recentDays[i]);
//            }
//        }
//        out.writeLong(size);
    }

    public void readFromTail() throws IOException {
//        out.close();
//        isClosed = true;
//        readIndex = writeIndex;
//        readFile = new RandomAccessFile(new File(logDir, readIndex + ""), "r");
//        in = new DataInputStream(new FileInputStream(readFile.getFD()));
//        readFile.seek(readFile.length() - 8);
//        readPosition = in.readLong();
//        readFile.seek(readPosition);
    }

    public long getTime() throws IOException {
        return in.readLong();
    }


    public boolean getInside() throws IOException {
        return in.readBoolean();
    }

    public boolean getLogStatus() throws IOException {
        return in.readBoolean();
    }

    public long getLastTime() throws IOException {
        return in.readLong();
    }

    public boolean getLastInside() throws IOException {
        return in.readBoolean();
    }

    public long[] getRecentDays() throws IOException {
        long[] recentDays = new long[10];
        for (int i = 0; i < recentDays.length; i++) {
            recentDays[i] = in.readLong();
        }
        return recentDays;
    }


    public boolean next() throws IOException {
//        if (readPosition - 8 > 0) {
//            readFile.seek(readPosition - 8);
//            readPosition = in.readLong();
//            readFile.seek(readPosition);
//            return true;
//        } else {
//            readIndex--;
//            if (readIndex >= 0) {
//                readFile = new RandomAccessFile(new File(logDir, readIndex + ""), "r");
//                in = new DataInputStream(new FileInputStream(readFile.getFD()));
//                readFile.seek(readFile.length() - 8);
//                readPosition = in.readLong();
//                readFile.seek(readPosition);
//                return true;
//            } else {
//                return false;
//            }
//        }
        return false;
    }

    public File getLogDir() {
        return null;
//        return logDir;
    }

    public File getFile() {
        return file;
    }

    public static void main(String[] args) throws IOException {
        EditLog log = new EditLog("100001019584781");
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 20000000; i++) {
            log.append(0L + i, true, 1L, true, new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        }
        log.readFromTail();
        long end = System.currentTimeMillis();
        System.out.println(String.format("cost:%dms", end - begin));
        System.out.println(log.getTime());
        System.out.println(log.getInside());
        log.next();
        System.out.println(log.getTime());
        System.out.println(log.getInside());
        for (int i = 2; i <= (10000000 - 50); i++) {
            log.next();
        }
        System.out.println(log.getTime());
        System.out.println(log.getInside());
    }


}
