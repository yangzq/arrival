package arrival.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 用户状态快照
 */
public class AccountSnapshot implements EditLog.Record, Comparable<AccountSnapshot> {
    private long start;
    private String imsi;
    private long time;
    private boolean inside;
    private boolean isSync;
    private long lastStart;
    private long lastTime;
    private boolean lastInside;
    private long[] lastRecentDays;
    private Accout.Status lastStatus;
    private int logNameIndex;
    private int startPosition;


    public AccountSnapshot() {
    }

    public AccountSnapshot(long start, String imsi, long time, boolean inside, boolean sync, long lastStart, long lastTime, boolean lastInside, long[] lastRecentDays, Accout.Status lastStatus) {
        this.start = start;
        this.imsi = imsi;

        this.time = time;
        this.inside = inside;
        isSync = sync;
        this.lastStart = lastStart;
        this.lastTime = lastTime;
        this.lastInside = lastInside;
        this.lastRecentDays = lastRecentDays;
        this.lastStatus = lastStatus;
    }


    public long getTime() {
        return time;
    }

    public boolean isInside() {
        return inside;
    }

    public long getLastStart() {
        return lastStart;
    }

    public long getLastTime() {
        return lastTime;
    }

    public boolean isLastInside() {
        return lastInside;
    }

    public long[] getLastRecentDays() {
        return lastRecentDays;
    }

    public Accout.Status getLastStatus() {
        return lastStatus;
    }

    public boolean isSync() {
        return isSync;
    }

    public void setSync(boolean sync) {
        isSync = sync;
    }

    public String getImsi() {
        return imsi;
    }

    @Override
    public int compareTo(AccountSnapshot o) {
        return (int) (time - o.time);
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(start);
        out.writeUTF(imsi);
        out.writeBoolean(inside);
        out.writeBoolean(isSync);
        if (isSync) {
            out.writeLong(lastStart);
            out.writeLong(lastTime);
            out.writeBoolean(lastInside);
            for (int i = 0; i < 10; i++) {
                out.writeLong(lastRecentDays[i]);
            }
            out.writeInt(lastStatus.ordinal());
        }
    }

    public static AccountSnapshot readFrom(DataInputStream in) throws IOException {
        AccountSnapshot snapshot = new AccountSnapshot();
        snapshot.start = in.readLong();
        snapshot.imsi = in.readUTF();
        snapshot.inside = in.readBoolean();
        snapshot.isSync = in.readBoolean();
        if (snapshot.isSync) {
            snapshot.lastStart = in.readLong();
            snapshot.lastTime = in.readLong();
            snapshot.lastInside = in.readBoolean();
            snapshot.lastRecentDays = new long[10];
            for (int i = 0; i < 10; i++) {
                snapshot.lastRecentDays[i] = in.readLong();
            }
            snapshot.lastStatus = Accout.Status.values()[in.readInt()];
        }
        return snapshot;
    }

    @Override
    public int getLogNameIndex() {
        return logNameIndex;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getStartPosition() {
        return startPosition;
    }

    public void setLogNameIndex(int logNameIndex) {
        this.logNameIndex = logNameIndex;
    }

    public void setStartPosition(int startPosition) {
        this.startPosition = startPosition;
    }

    public long getStart() {
        return start;
    }
}
