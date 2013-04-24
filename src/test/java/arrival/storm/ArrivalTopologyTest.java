package arrival.storm;

import arrival.util.EventTypeConst;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.lang.Thread.sleep;

/**
 * ArrivalTopology Tester
 */
public class ArrivalTopologyTest {


    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {

    }

    /**
     * Method: main(String[] args)
     */
    @Test
    public void testMain() throws Exception {
        TopologyBuilder builder = arrival.storm.ArrivalTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("arrival", conf, builder.createTopology());

        sleep(4 * 1000);
        Sender sender = new Sender(5003);

        sender.send("arrival1", EventTypeConst.EVENT_TURN_OFF, "2013-01-04 08:00:00", "lac", "home");
        sender.send("arrival1", EventTypeConst.EVENT_TURN_ON, "2013-01-04 08:01:00", "lac", "airport");
        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-04 08:10:00", "lac", "airport");
        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-04 08:20:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALLED, "2013-01-04 10:01:00", "lac", "home");
        sleep(1000);
        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-04 09:59:00", "lac", "home");

//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-05 08:10:00", "lac", "airport");
//        sender.send("normal1", EventTypeConst.EVENT_CALL, "2013-01-05 23:59:00", "lac", "home");
//        sender.send("normal1", EventTypeConst.EVENT_CALL, "2013-01-06 01:01:00", "lac", "home");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-06 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-07 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-07 09:00:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-07 10:00:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-08 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-09 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-10 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-11 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-12 08:10:00", "lac", "airport");
//        sender.send("arrival1", EventTypeConst.EVENT_CALL, "2013-01-13 08:10:00", "lac", "airport");

        sleep(1 * 1000);
        sender.close();
        sleep(1 * 1000);
        cluster.shutdown();
    }

    @Test
    public void testMain2() throws Exception {
        TopologyBuilder builder = arrival.storm.ArrivalTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("arrival", conf, builder.createTopology());

        sleep(4 * 1000);
        Sender sender = new Sender(5003);


        sleep(1 * 1000);
        sender.close();
        sleep(1 * 1000);
        cluster.shutdown();
    }

    @Test
    public void testMain3() throws Exception {
        TopologyBuilder builder = arrival.storm.ArrivalTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 50);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("arrival", conf, builder.createTopology());

        sleep(4 * 1000);
        Sender sender = new Sender(5003);
        BufferedReader reader = null;
        try {
//            String filePath = "/tmp/100001002999342.csv";
            String filePath = "/data.csv";
            reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(filePath)));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String signal = line.substring(0, line.indexOf(",2013-"));
                sender.send(signal);
                System.out.println("send:" + line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        sleep(1000);
        sender.close();

        cluster.shutdown();
    }

    private static class Sender extends IoHandlerAdapter {
        private final IoConnector connector;
        private final IoSession session;

        private Sender(int port) {
            connector = new NioSocketConnector();
            connector.setHandler(this);
            connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
            ConnectFuture future = connector.connect(new InetSocketAddress(port));
            future.awaitUninterruptibly();
            session = future.getSession();
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            super.messageReceived(session, message);
        }

        public void send(String imsi, String event, String time, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(event).append(",").append(getTime(time)).append(",").append(loc).append(",").append(cell));
        }

        public void send(String imsi, String event, long time, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(event).append(",").append(time).append(",").append(loc).append(",").append(cell));
        }

        public void send(String signal) throws ParseException, InterruptedException {
            session.write(signal).await();
        }

        public void close() throws InterruptedException {
            session.close(false).await();
            connector.dispose();
        }
    }

    private static long getTime(String s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(s + " +0000").getTime();
    }

    private static String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(getTime(1356999187197L));
        System.out.println(getTime(1357001569894L));
    }
}
