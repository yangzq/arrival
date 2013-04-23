package arrival.util;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 写日志。
 */
public class EditLog<T extends EditLog.Record> implements Serializable {

    private transient final Method readMethod;
    private final File logDir;//EditLog文件夹名
    private CurrentLog currentLog; //当前的日志

    private transient final HashMap<String, AtomicInteger> userRecordCount = new HashMap<String, AtomicInteger>();

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

    public void append(T record) {
        try {
            if (currentLog.out.size() > 128 * 1024 * 1024) {
                currentLog.out.close();
                currentLog = new CurrentLog(logDir, ++currentLog.logNameIndex, 0, userRecordCount);
            }
            AtomicInteger count = userRecordCount.get(record.getImsi());
            if (count == null) {
                count = new AtomicInteger(-1);
                userRecordCount.put(record.getImsi(), count);
            }
//        record.setSync(false);
            record.setSync((count.incrementAndGet() & 127) == 0); //等效于(count.incrementAndGet() % 128) == 0
            int size = currentLog.out.size();
            record.writeTo(currentLog.out);
            currentLog.out.writeInt(size);
            if (size - currentLog.syncPositon > 8 * 1024 * 1024) {
                currentLog.syncPositon = size;
            }
            currentLog.out.writeInt(currentLog.syncPositon);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //    public boolean forEachFromTail(RecordProcessor<T> processor) {//如果某次processor返回false，则终止，返回false
//        FileInputStream in = null;
//        try {
//            currentLog.out.close();
//            int logNameIndex = currentLog.logNameIndex;
//            File logFile = new File(logDir, String.valueOf(logNameIndex));
//            boolean isContinue = true;
//            while (logFile.exists() && isContinue) {
//                if (in != null) {
//                    in.close();
//                }
//                RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
//
//                in = new FileInputStream(randomAccessFile.getFD());
//                DataInputStream postionInputStream = new DataInputStream(in);
//
//                int position= (int) (randomAccessFile.length()-4);
//                randomAccessFile.seek(position);
//                int startPosition = postionInputStream.readInt();
//                position = position - 4;
//                randomAccessFile.seek(position);
//                int endPosition = postionInputStream.readInt();
//                while (isContinue) { //读取从syncPosition 到 syncEndPosition的Record
//                    DataInputStream recordInputStream = new DataInputStream(new BufferedInputStream(in));
//                    LinkedList<Record> records = new LinkedList<Record>();
//                    randomAccessFile.seek(startPosition);
//                    int start = startPosition;
//                    while (start < (endPosition - 1)) {
//                        Record record = (Record) readMethod.invoke(0, recordInputStream);
//                        start = recordInputStream.readInt();
//                        record.setStartPosition(start);
//                        record.setLogNameIndex(logNameIndex);
//                        records.push(record);
//                        recordInputStream.readInt();// sycn postion
//                    }
//                    for (Record record : records) {
//                        isContinue = processor.on((T) record);
//                        if (!isContinue) break;
//                    }
//
//                    if (startPosition == 0) {
//                        logFile = new File(logDir, String.valueOf(--logNameIndex));
//                        break;
//                    }
//
//                    position = startPosition - 4;
//                    randomAccessFile.seek(position);
//                    startPosition = postionInputStream.readInt();
//                    position = position - 4;
//                    randomAccessFile.seek(position);
//                    endPosition = postionInputStream.readInt();
//                }
//            }
//            return isContinue;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            if (in != null) {
//                try {
//                    in.close();
//                } catch (IOException e) {
//                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
//            }
//        }
//    }
    public boolean forEachFromTail(RecordProcessor<T> processor){
        try {
            currentLog.out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int logNameIndex = currentLog.logNameIndex;
        File logFile = new File(logDir, String.valueOf(logNameIndex));
        boolean isContinue = true;
        while (logFile.exists() && isContinue) {
            isContinue = readOneFile(logFile, processor, logNameIndex);
            logNameIndex--;
            logFile = new File(logDir, String.valueOf(logNameIndex));
        }
        return isContinue;
    }

    private boolean readOneFile(File logFile, RecordProcessor<T> processor, int logNameIndex) {
        FileInputStream in = null;
        try {
            if (!logFile.exists()) return false;
            RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
            in = new FileInputStream(randomAccessFile.getFD());
            DataInputStream positionInputStream = new DataInputStream(in);
            int position = (int) (randomAccessFile.length() - 4);
            randomAccessFile.seek(position);
            int startPosition = positionInputStream.readInt();
            position = position - 4;
            randomAccessFile.seek(position);
            int endPosition = positionInputStream.readInt();
            boolean isContinue = true;
            while (isContinue) { //读取从syncPosition 到 syncEndPosition的Record
                DataInputStream recordInputStream = new DataInputStream(new BufferedInputStream(in));
                LinkedList<Record> records = new LinkedList<Record>();
                randomAccessFile.seek(startPosition);
                int start = startPosition;
                while (start < (endPosition - 1)) {
                    Record record = (Record) readMethod.invoke(0, recordInputStream);
                    start = recordInputStream.readInt();
                    record.setStartPosition(start);
                    record.setLogNameIndex(logNameIndex);
                    records.push(record);
                    recordInputStream.readInt();// sycn postion
                }
                for (Record record : records) {
                    isContinue = processor.on((T) record);
                    if (!isContinue) break;
                }

                if (startPosition == 0) {
                    break;
                }

                position = startPosition - 4;
                randomAccessFile.seek(position);
                startPosition = positionInputStream.readInt();
                position = position - 4;
                randomAccessFile.seek(position);
                endPosition = positionInputStream.readInt();
            }
            return isContinue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void seek(int logNameIndex, int startPosition) {
        try {
            currentLog.out.close();
            int maxIndex = currentLog.logNameIndex;
            while (maxIndex > logNameIndex) {
                boolean result = new File(logDir, String.valueOf(maxIndex)).delete();
                maxIndex--;
            }
            File newFile = File.createTempFile("storm", "tmp");
            newFile.delete();
            new File(logDir, String.valueOf(logNameIndex)).renameTo(newFile);
            currentLog = new CurrentLog(logDir, logNameIndex, 0, userRecordCount);
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(newFile)));
            while(true){
                T record = (T) readMethod.invoke(0, in);
                int thisStartPosition =in.readInt(); //startPostion
                in.readInt();// sycn postion
                if(thisStartPosition==startPosition){
                    break;
                }
                append(record);
//                record.writeTo(currentLog.out);
            }
            in.close();
            newFile.delete();
        } catch (Exception e) {
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
        private int syncPositon = 0;

        public CurrentLog(File logDir, int logNameIndex, int position, HashMap userRecordCount) throws IOException {
            userRecordCount.clear();
            this.logNameIndex = logNameIndex;
            int bufferedSize = 1024 * 1024;
            if (position != 0) {
                File oldFile = new File(logDir, String.valueOf(logNameIndex));
                File newFile = File.createTempFile("storm", "tmp");
                oldFile.renameTo(newFile);
                this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(logDir, String.valueOf(logNameIndex))), bufferedSize));

//                this.out = new StartWithPositionDataOutputStream(new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD()), bufferedSize), position);
            } else {
                this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(logDir, String.valueOf(logNameIndex))), bufferedSize));
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

    public static void main(String[] args) {
        long a = 0;
        long b = System.currentTimeMillis();
        for (int i = 0; i < 1000000000; i++) {
            if ((i & 255) == 0) {
                a++;
            }
        }
    }
}

