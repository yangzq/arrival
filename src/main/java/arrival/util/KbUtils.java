package arrival.util;

/**
 *
 */
public class KbUtils {
    static class SingletonHolder {
        static KbUtils instance = new KbUtils();
    }

    public static KbUtils getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * 检查用户ARPU是否满足“三月平均值>50”，暂时设定为全部返回满足
     *
     * @param imsi
     * @return
     */
    public boolean checkARPU(String imsi) {
        return true;
    }

    /**
     * 检查用户是否在机场，信令cell字段值为airport时在机场
     * @param lac
     * @param cell
     * @return
     */
    public boolean isInAirport(String lac, String cell) {
        return "airport".equals(cell);
    }
}
