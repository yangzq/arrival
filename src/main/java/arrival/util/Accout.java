package arrival.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static arrival.util.TimeUtil.ONE_DAY;
import static arrival.util.TimeUtil.ONE_HOUR;

/**
 * 账户，代表一个用户在某个统计方式下的状态，比如他应该是Arrival?Worker?Normal?
 * 每个账户会保存三个状态：
 * 1. 最后一次信令时间：lastTime
 * 2. 最后一次信令时的状态：lastInside
 * 3. 最近10天的停留时间：lastRecentDays
 * <p/>
 * 每次账户收到一个新的信令，都会记录到EditLog里。当发现新收到的信令比之前的信令早时，从EditLog读取之前的一个同步点，
 * 从这个同步点开始，后续的记录重新排序后重新计算。
 */
public class Accout {


    public enum Status {
        Worker, Normal, Arrival
    }

    private final long start;
    private final String imsi;
    private final UserGroup.Listener listener;
    private boolean bootStatus = false;

    private long lastStart; //上次统计开始时间,绝对时间。默认值为1970年的早8点
    private long lastTime = 0; // 上次信令时间
    private boolean lastInside = false; // 上次信令是否在里面
    private long[] lastRecentDays = new long[30]; // 最近30天的停留时间
    private Status lastStatus = Status.Normal; //上次用户状态


    private final EditLog<AccountSnapshot> editLog;

    public Accout(long start, String imsi, UserGroup.Listener listener, EditLog<AccountSnapshot> editLog) throws IOException {
        this.start = start;
        this.imsi = imsi;
        this.listener = listener;
        this.lastStart = start;
        this.editLog = editLog;
    }

    public void onSignal(final long time, String eventType, String lac, String cell) throws IOException {
        boolean isInside = KbUtils.getInstance().isInAirport(lac, cell);
        if (isInside && eventType.equals(EventTypeConst.EVENT_TURN_ON)){
            bootStatus = true;
        }
        AccountSnapshot accountSnapshot = new AccountSnapshot(start, imsi, time, isInside, true, lastStart, lastTime, lastInside, lastRecentDays, lastStatus);
        this.editLog.append(accountSnapshot);
        if (time >= lastTime) {//正序
            order(time, isInside);
        } else {//乱序,很少发生，不需要考虑效率
            final List<AccountSnapshot> misOrderSnapshots = new ArrayList<AccountSnapshot>();
            misOrderSnapshots.add(accountSnapshot);
            boolean isFindSync = !editLog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
                @Override
                public boolean on(AccountSnapshot record) {
                    misOrderSnapshots.add(record);
                    return !(record.isSync() && record.getTime() < time && start == record.getStart() && imsi.equals(record.getImsi())); //找到同步点就不再找了
                }
            });
            Collections.sort(misOrderSnapshots);

            if (isFindSync) {
                AccountSnapshot sync = misOrderSnapshots.get(0);
                this.lastInside = sync.isLastInside();
                this.lastTime = sync.getLastTime();
                this.lastRecentDays = sync.getLastRecentDays();
                this.lastStart = sync.getLastStart();
                this.lastStatus = sync.getLastStatus();
                this.editLog.seek(sync.getLogNameIndex(), sync.getStartPosition()); //转到指定位置，并且清空后面的数据
            } else {
                this.lastInside = false;
                this.lastTime = 0;
                this.lastRecentDays = new long[30];
                this.lastStart = this.start;
                this.lastStatus = Status.Normal;
            }

            for (AccountSnapshot snapshot : misOrderSnapshots) {
                if (snapshot.getImsi().equals(imsi) && start == snapshot.getStart()) {
                    this.editLog.append(new AccountSnapshot(start, imsi, snapshot.getTime(), snapshot.isInside(), true, lastStart, lastTime, lastInside, lastRecentDays, lastStatus));
                    order(snapshot.getTime(), snapshot.isInside());
                } else {
                    this.editLog.append(snapshot);
                }
            }


        }
        check(time);
    }

    private void order(long time, boolean inside) {
        do {
            if (lastInside) { // 上次在景区则添加本次停留时间
                lastRecentDays[29] += Math.max((Math.min(time, lastStart + 24 * ONE_HOUR) - lastTime), 0);
            }
            if (time < lastStart + ONE_DAY) {
                lastTime = time;
            } else {
                lastTime = lastStart + ONE_DAY;
                for (int i = 0; i < lastRecentDays.length - 1; i++) {
                    lastRecentDays[i] = lastRecentDays[i + 1];
                }
                lastRecentDays[29] = 0;
                lastStart += ONE_DAY;
            }
        } while (time > lastStart + ONE_DAY - 1);
        lastInside = inside;
    }

    public boolean isWorker() {
        return lastStatus == Status.Worker;
    }

    public void updateGlobleTime(Long globalTime) {
        if (lastInside && globalTime > lastTime) {
            order(globalTime, lastInside);
            check(globalTime);
        }
    }

    private void check(long time) {
        int i = 0;
        long timeSum = 0L;
        for (long o : lastRecentDays) {
            if ((++i > 9) || ((timeSum += o) > (50 * ONE_HOUR - 1))) {
                if (lastStatus != Status.Worker) {
                    this.listener.onAddWorker(time, imsi, lastStatus);
                    lastStatus = Status.Worker;
                }
            }
        }
        if (lastInside) {
            if (lastStatus != Status.Worker) {
                this.listener.onAddArrival(time, imsi, lastStatus);
                lastStatus = Status.Arrival;
            }
        } else {
            if (lastStatus != Status.Worker) {
                this.listener.onAddNormal(time, imsi, lastStatus);
                lastStatus = Status.Normal;
            }
        }
    }
}
