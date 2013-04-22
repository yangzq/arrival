package arrival.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static arrival.util.TimeUtil.ONE_DAY;
import static arrival.util.TimeUtil.ONE_HOUR;
import static arrival.util.TimeUtil.getTime;

/**
 * 账户，代表一个用户在某个统计方式下的状态，比如他应该是Arrival?Worker?Normal?
 * 每个账户会保存三个状态：
 * 1. 最后一次信令时间：lastTime
 * 2. 最后一次信令时的状态：lastInside
 * 3. 最近30天的停留时间：lastRecentDays
 * <p/>
 * 每次账户收到一个新的信令，都会记录到EditLog里。当发现新收到的信令比之前的信令早时，从EditLog读取之前的一个同步点，
 * 从这个同步点开始，后续的记录重新排序后重新计算。
 */
public class Accout {

    public enum Status {
        Worker, Normal, Arrival
    }

    private final String imsi;
    private final UserGroup.Listener listener;

    private boolean bootStatus = false; // 开机状态：是否在机场开机
    private long bootTime = 0L;

    private long lastStart; //上次统计开始时间，绝对时间。默认值为1970/01/01的0点
    private long lastTime = 0; // 上次信令时间
    private boolean lastInside = false; // 上次信令是否在里面
    private long[] lastRecentDays = new long[30]; // 最近30天的停留时间
    private Status lastStatus = Status.Normal; //上次用户状态，默认为Normal

    private final EditLog<AccountSnapshot> editLog;
    private AtomicInteger invokeOrderTime = new AtomicInteger();

    public Accout(String imsi, UserGroup.Listener listener, EditLog<AccountSnapshot> editLog) throws IOException {
        this.imsi = imsi;
        this.listener = listener;
        this.lastStart = 0;
        this.editLog = editLog;
    }

    public void onSignal(final long time, String eventType, String lac, String cell) throws IOException {
        boolean isInside = KbUtils.getInstance().isInAirport(lac, cell);
        if (isInside && eventType.equals(EventTypeConst.EVENT_TURN_ON)) {
            bootStatus = true;
            bootTime = time;
        }
        AccountSnapshot accountSnapshot = new AccountSnapshot(imsi, time, isInside, true, lastStart, lastTime, lastInside, lastRecentDays, lastStatus);
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
                    return !(record.isSync() && record.getTime() < time && imsi.equals(record.getImsi())); //找到同步点就不再找了
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
//                this.lastStart = this.start;
                this.lastStatus = Status.Normal;
            }

            for (AccountSnapshot snapshot : misOrderSnapshots) {
                if (snapshot.getImsi().equals(imsi)) {
                    this.editLog.append(new AccountSnapshot(imsi, snapshot.getTime(), snapshot.isInside(), true, lastStart, lastTime, lastInside, lastRecentDays, lastStatus));
                    order(snapshot.getTime(), snapshot.isInside());
                } else {
                    this.editLog.append(snapshot);
                }
            }


        }
        if (!isInside){
            if (lastStatus == Status.Arrival && time < bootTime + 2 * ONE_HOUR){
                System.out.println("send sms to imsi:" + imsi + " on " + getTime(time) + "/" + time);
            }
            bootStatus = false;
            bootTime = 0L;
        }
        check(time);
    }

    private void order(long time, boolean inside) {
        do {
            if (lastInside) { // 上次在景区则添加本次停留时间
                long delta = Math.max((Math.min(time, lastStart + 24 * ONE_HOUR) - lastTime), 0);
                lastRecentDays[29] += delta;
                System.out.println("add delta:" + delta + " on time:" + time +"/" + getTime(time)
                        + " lastStart:" + getTime(lastStart) + " lastTime:" + getTime(lastTime));
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
        if (invokeOrderTime.incrementAndGet() == 1L){
            lastTime = time;
        }
        lastInside = inside;
        System.out.println(ArrayUtils.toString(lastRecentDays));
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
            if (o > 0) {
                if ((++i > 9) || ((timeSum += o) > (50 * ONE_HOUR - 1))) {
                    if (lastStatus != Status.Worker) {
                        this.listener.onAddWorker(time, imsi, lastStatus);
                        lastStatus = Status.Worker;
                    }
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
