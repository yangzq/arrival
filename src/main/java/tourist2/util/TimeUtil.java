package tourist2.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-2-28
 * Time: 下午12:48
 *
 */
public class TimeUtil {
  public static final long ONE_SECOND = 1000;
  public static final long ONE_MINUTE = 60 * ONE_SECOND;
  public static final long ONE_HOUR = 60 * ONE_MINUTE;
  public static final long ONE_DAY = 24 * ONE_HOUR;
    /**
     * 打印出零时区的时间： 0 -> 1970-01-01 00:00:00
     *
     * @param s
     * @return
     */
    public static String getTime(long s) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) {
        System.out.println(getTime(0));
        System.out.println(getDays(1357028262540L));
        System.out.println(getTime(getDays(1357028262540L) + 8 * ONE_HOUR));
    }

    public static String time2HHMMSS(long time) {
        StringBuilder sb = new StringBuilder();
        sb.append(longTo2c(time / ONE_HOUR));
        sb.append(":");
        sb.append(longTo2c((time % ONE_HOUR) / ONE_MINUTE));
        sb.append(":");
        sb.append(longTo2c((time % ONE_MINUTE) / ONE_SECOND));
        return sb.toString();
    }
    public static String time2HHMM(long time) {
        StringBuilder sb = new StringBuilder();
        sb.append(longTo2c(time / ONE_HOUR));
        sb.append(":");
        sb.append(longTo2c((time % ONE_HOUR) / ONE_MINUTE));
        return sb.toString();
    }

    public static String longTo2c(long l) {
        String s = Long.toString(l);
        return s.length() == 2 ? s : "0" + s;
    }

    // 100001000414086,1357028262540,shy,stadium,2013-01-01 08:17:42.540
    public static long getDays(long time) {
        return time - time % ONE_DAY;
    }
}
