package tourist2.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tourist2.util.NioServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class SignalingSpout extends BaseRichSpout {
    public static final String SIGNALING = "signaling";
    public static final String TIME = "time";
    private static Logger logger = LoggerFactory.getLogger(SignalingSpout.class);
    LinkedBlockingQueue<String> queue = null;
    NioServer nioServer = null;
    private SpoutOutputCollector spoutOutputCollector;
    private final long updateTimeInterval = 100;
    private static long i = 0L;
    private int port;

    final AtomicInteger count = new AtomicInteger();
    final AtomicLong ss = new AtomicLong(System.currentTimeMillis());

    public SignalingSpout(int i) {
        this.port = i;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIGNALING, new Fields("imsi", "time", "loc", "cell")); //根据用户分组发送到不同机器
        outputFieldsDeclarer.declareStream(TIME, new Fields("time")); //发送到所有机器
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(10000);
//        final AtomicInteger count = new AtomicInteger();
//        final AtomicLong ss = new AtomicLong(System.currentTimeMillis());
        NioServer.Listener listener = new NioServer.Listener() {
            @Override
            public void messageReceived(String message) throws Exception {
//        logger.info(String.format("spout received:%s", message));
                queue.put(message); // 往队列中添加信令时阻塞以保证数据不丢失
//                if (count.incrementAndGet() % 100000 == 1) {
//                    long en = System.currentTimeMillis();
//                    logger.info("cost:" + (en - ss.get()));
//                    ss.set(en);
//                }


            }
        };
        nioServer = new NioServer(port, listener);
        try {
            nioServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        String message = null;
        while ((message = queue.poll()) != null) {
//            if (count.incrementAndGet() % 100000 == 1) {
//                long en = System.currentTimeMillis();
//                logger.info("cost3:" + (en - ss.get()));
//                ss.set(en);
//            }
//        spoutOutputCollector.emit(TIME, new Values(message));
            String[] columns = message.split(",");
            long time = Long.parseLong(columns[1]);
            spoutOutputCollector.emit(SIGNALING, new Values(columns[0], time, columns[2], columns[3]));
            if (i % updateTimeInterval == 0L) {
                spoutOutputCollector.emit(TIME, new Values(time));
            }
            i++;
////      System.out.println("Tuple amount:" + i);
        }
    }

    @Override
    public void close() {
        try {
            nioServer.stop();
        } catch (IOException e) {
            logger.warn("error when stop nioServer", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        logger.info("ack(): " + msgId.toString());
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        logger.error("fail(): " + msgId.toString());
    }
}
