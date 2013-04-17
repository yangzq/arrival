package tourist2.util;

import org.apache.commons.collections.Factory;
import org.apache.commons.collections.map.LazyMap;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 写日志。
 */
public class EditLog<T extends EditLog.Record> implements Serializable {

    private transient final Method readMethod;
    private final File logDir;//EditLog文件夹名
    private CurrentLog currentLog; //当前的日志

    private transient final LazyMap userRecordCount = (LazyMap) LazyMap.decorate(new HashMap(),new Factory() {
        @Override
        public Object create() {
            return new AtomicInteger(0);
        }
    });
    public EditLog(File logDir, Class<?> recordClass) {
        try {
            this.logDir = logDir;
            FileUtils.deleteDirectory(logDir);
            logDir.mkdirs();
            currentLog = new CurrentLog(logDir, 0, 0, userRecordCount);
            readMethod = recordClass.getMethod("readFrom", DataInputStream.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void append(T record) throws IOException {
        AtomicInteger count = (AtomicInteger) userRecordCount.get(record.getImsi());
        record.setSync(count.incrementAndGet()%100==1);
        if (currentLog.out.size() > 512 * 1024 * 1024) {
            currentLog.out.close();
            currentLog = new CurrentLog(logDir, ++currentLog.logNameIndex, 0,userRecordCount);
        }
        int size = currentLog.out.size();
        record.writeTo(currentLog.out);
        currentLog.out.writeInt(size);
    }

    public boolean forEachFromTail(RecordProcessor<T> processor) {//如果某次processor返回false，则终止，返回false
        try {
            currentLog.out.close();
            int logNameIndex = currentLog.logNameIndex;
            File logFile = new File(logDir, String.valueOf(logNameIndex));
            boolean isContinue = true;
            while (logFile.exists() && isContinue) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(randomAccessFile.getFD())));
                randomAccessFile.seek(logFile.length() - 4);
                int position = in.readInt();
                while (position == -1) {
                    position = in.readInt();
                }
                while (isContinue) {
                    randomAccessFile.seek(position);
                    Record record = (Record) readMethod.invoke(0, in);
                    record.setLogNameIndex(logNameIndex);
                    record.setStartPosition(position);
                    isContinue = processor.on((T) record);
                    if (position == 0) {
                        break;
                    }
                    randomAccessFile.seek(position - 4);
                    position = in.readInt();
                }
            }
            return isContinue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void seek(int logNameIndex, int startPosition) {
        try {
            currentLog.out.close();
            currentLog = new CurrentLog(logDir, ++currentLog.logNameIndex, startPosition, userRecordCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            currentLog.out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public interface RecordProcessor<T extends Record> {
        boolean on(T record);
    }

    public interface Record {
        void writeTo(DataOutputStream out) throws IOException;
        String getImsi();
        int getStartPosition();

        int getLogNameIndex();
        void setSync(boolean sync);

        void setLogNameIndex(int logNameIndex);

        void setStartPosition(int startPosition);
    }

    private static class CurrentLog implements Serializable {
        private int logNameIndex = 0;// EditLog文件序号，从0开始，每隔512M换一个文件
        private transient DataOutputStream out;//当前EditLog

        public CurrentLog(File logDir, int logNameIndex, int position, LazyMap userRecordCount) throws IOException {
            userRecordCount.clear();
            this.logNameIndex = logNameIndex;
            if (position != 0) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(new File(logDir, String.valueOf(logNameIndex)), "rw");
                randomAccessFile.seek(position);
                this.out = new StartWithPositionDataOutputStream(new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD())), position);
                while (out.size() < randomAccessFile.length()) {
                    out.writeInt(-1);
                    out.close();
                }
                randomAccessFile.seek(position);
                this.out = new StartWithPositionDataOutputStream(new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD())), position);
            } else {
                this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(logDir, String.valueOf(logNameIndex)))));
            }
        }
    }

    private static class StartWithPositionDataOutputStream extends DataOutputStream {
        public StartWithPositionDataOutputStream(OutputStream out, int written) {
            this(out);
            this.written = written;
        }

        public StartWithPositionDataOutputStream(OutputStream out) {
            super(out);
        }
    }
}
