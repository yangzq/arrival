package tourist2.util;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-1-5
 * Time: 下午12:18
 *
 */
public class KbUtils {
    static class SingletonHolder {
        static KbUtils instance = new KbUtils();
    }

    public static KbUtils getInstance() {
        return SingletonHolder.instance;
    }

    public boolean isInside(String loc, String cell) {
        return "tourist".equals(cell);  //默认用户不在景区
    }

}
