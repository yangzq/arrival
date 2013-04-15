package tourist2.util;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;
import static tourist2.util.TimeUtil.*;

/**
 * 账户，代表一个用户在某个统计方式下的状态，比如（如果按照8~18点统计，他应该是Tourist?Worker?Normal?
 * 每个账户会保存三个状态：
 * 1. 最后一次信令时间：lastTime
 * 2. 最后一次信令时的状态：lastInside
 * 3. 最近10天的停留时间：recentDays
 * <p/>
 * 每次账户收到一个新的信令，都会记录到EditLog里。当发现新收到的信令比之前的信令早时，从EditLog读取之前的一个同步点，
 * 从这个同步点开始，后续的记录重新排序后重新计算。
 */
public class Accout {
    public enum Status {
        Worker, Normal, Tourist
    }

    private final long start;
    private String imsi;
    private final UserGroup.Listener listener;
    private long lastStart;
    private int daysThreashold;
    private long lastTime = 0; // 最后一次信令时间
    private boolean lastInside = false; // 最后一次信令时的状态
    private long[] recentDays = new long[10]; // 最近10天的停留时间
    private Status status = Status.Normal;
    private final EditLog editLog;

    public Accout(long start, String imsi, UserGroup.Listener listener, int daysThreashold) throws IOException {
        this.start = start;
        this.imsi = imsi;
        this.listener = listener;
        this.lastStart = start;
        this.daysThreashold = daysThreashold;
        this.editLog = new EditLog(imsi);
    }

    public void onSignal(long time, String loc, String cell) throws IOException {
        lastStart = getDays(time) + start;
        System.out.println(String.format("[lastStart]%s~%s", imsi, getTime(lastStart)));
        boolean isInside = KbUtils.getInstance().isInside(loc, cell);
        this.editLog.append(time, isInside, lastTime, lastInside, recentDays);
        if (time >= lastTime) {//正序
            order(time, isInside);
        } else {//乱序,很少发生，不需要考虑效率
            List<Object[]> olds = new ArrayList<Object[]>();
            editLog.readFromTail();
            long atime = 0;
            boolean aLogStatus = false;
            do {
                atime = editLog.getTime();
                boolean aInside = editLog.getInside();
                olds.add(new Object[]{atime, aInside});
                aLogStatus = editLog.getLogStatus();
            } while (atime > (time - 20*ONE_MINUTE) && editLog.next());

            while(!aLogStatus&& editLog.next()){
                atime = editLog.getTime();
                boolean aInside = editLog.getInside();
                olds.add(new Object[]{atime, aInside});
                aLogStatus = editLog.getLogStatus();
            }


            //修改为之前的状态
            this.lastTime = editLog.getLastTime();
            this.lastInside = editLog.getLastInside();
            this.recentDays = editLog.getRecentDays();

            olds.add(new Object[]{time, loc, cell});
            Collections.sort(olds, new Comparator<Object[]>() { //由小到大排序
                @Override
                public int compare(Object[] o1, Object[] o2) {
                    return (int) ((Long) o1[0] - (Long) o2[0]);
                }
            });
            for (Object[] old : olds) {
                order((Long) old[0], KbUtils.getInstance().isInside(loc, cell));
            }
        }
        check(time);
    }

    private void order(long time, boolean inside) {
//        System.out.println("");
        do {
            if (lastInside) { // 上次在景区则添加本次停留时间
                if (start == 8 * ONE_HOUR) {
                    recentDays[9] += Math.max((Math.min(time, lastStart + 10 * ONE_HOUR) - lastTime),0);
                } else if (start == 18 * ONE_HOUR) {
                    recentDays[9] += Math.max((Math.min(time, lastStart + 14 * ONE_HOUR) - lastTime),0);
                }

            }
            if (time < lastStart + ONE_DAY) {
                lastTime = time;
            } else {
                lastTime = lastStart + ONE_DAY;
                for (int i = 0; i < recentDays.length - 1; i++) {
                    recentDays[i] = recentDays[i + 1];
                }
                recentDays[9] = 0;
                lastStart += ONE_DAY;
            }
        } while (time > lastStart + ONE_DAY - 1);
        lastInside = inside;
//        System.out.println(format("time: %s, lastStart: %s ,lastTime: %s",getTime(time),getTime(lastStart),getTime(lastTime)));
    }

    public static void main(String[] args) throws IOException {
       EditLog editLog = new EditLog("111");
        editLog.append(1,true,1,true,new long[10]);
        editLog.append(4,true,1,true,new long[10]);
        editLog.readFromTail();
        long time =2;
        long atime=0;
        List<Object[]> olds = new ArrayList<Object[]>();
        boolean aLogStatus=false;
        do {
            atime = editLog.getTime();
            boolean aInside = editLog.getInside();
            olds.add(new Object[]{atime, aInside});
            aLogStatus = editLog.getLogStatus();
        } while (atime > time && editLog.next());

        while(!aLogStatus&& editLog.next()){
            atime = editLog.getTime();
            boolean aInside = editLog.getInside();
            olds.add(new Object[]{atime, aInside});
            aLogStatus = editLog.getLogStatus();
        }
        System.out.println(aLogStatus);
        //修改为之前的状态
        System.out.println( editLog.getLastTime());
        editLog.getLastInside();
        editLog.getLogStatus();
        editLog.getRecentDays();
    }

    private static String getTime(long s) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public boolean isWorker() {
        return status == Status.Worker;
    }

    public void updateGlobleTime(Long globalTime) {
        if (globalTime > lastTime) {
            order(globalTime, lastInside);
        }
        check(globalTime);
    }

    private void check(long time) {
//        StringBuffer sb = new StringBuffer();
//        for (long a : recentDays) sb.append(a).append(",");
//        System.out.println(getTime(time)+":"+daysThreashold + ":" + sb);
        int i = 0;
        for (long o : recentDays) {
            if (o > daysThreashold * ONE_HOUR) {
                if (++i > 4) {
                    if (status != Status.Worker) {
                        this.listener.onAddWorker(time, imsi, status);
                        status = Status.Worker;
                    }
                }
            }
        }
        if (lastInside) {
            if (status != Status.Worker) {
                this.listener.onAddTourist(time, imsi, status);
                status = Status.Tourist;
            }
        } else {
            if (status != Status.Worker) {
                this.listener.onAddNormal(time, imsi, status);
                status = Status.Normal;
            }
        }
    }

    public EditLog getEditLog() {
        return editLog;
    }
}
