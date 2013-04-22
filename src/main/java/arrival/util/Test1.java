package arrival.util;

import org.apache.commons.lang.ArrayUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-22
 * Time: 下午9:30
 */
public class Test1 {
    public static void main(String[] args) {
//        long[] longs = {123L, 234L,345L};
//        System.out.println(ArrayUtils.toString(longs));
        AtomicInteger atomicInteger = new AtomicInteger();
        System.out.println(atomicInteger.incrementAndGet());
        System.out.println(atomicInteger.incrementAndGet());
        System.out.println(atomicInteger.incrementAndGet());
    }
}
