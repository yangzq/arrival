package tourist2.storm;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-16
 * Time: 下午2:12
 */
public class Test1 {
    public static void main(String[] args) {
        long time = 1357228921422L;
        long circle = 1357236000000L;
        long cross = 1357228921000L;
        System.out.println("time = " + time + "/" + getTime(time));
        System.out.println("circle = " + circle + "/" + getTime(circle));
        System.out.println("cross = " + cross + "/" + getTime(cross));

    }

    private static String getTime(long s) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }
}
