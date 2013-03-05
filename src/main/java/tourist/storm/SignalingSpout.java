package tourist.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tourist.util.NioServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 *
 */
public class SignalingSpout extends BaseRichSpout {
    public static final String SIGNALING = "signaling";
    private static Logger logger = LoggerFactory.getLogger(SignalingSpout.class);
    LinkedBlockingQueue<String> queue = null;
    NioServer nioServer = null;
    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIGNALING, new Fields("imsi", "time", "loc", "cell"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(1000);
        NioServer.Listener listener = new NioServer.Listener() {
            @Override
            public void messageReceived(String message) throws Exception {
                logger.info(String.format("spout received:%s", message));
//                queue.offer(message);
                queue.put(message); // 往队列中添加信令时阻塞一保证数据不丢失
            }
        };
        nioServer = new NioServer(5001, listener);
        try {
            nioServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
//        if (logger.isDebugEnabled()) {
//            logger.info(String.format("spout nextTuple() time: %s", System.currentTimeMillis()));
//        }
        String message =null;
        while ((message = queue.poll()) != null) {
            String[] columns = message.split(",");
            Values tuple = new Values(columns[0], Long.parseLong(columns[1]), columns[2], columns[3]);
            if (logger.isDebugEnabled()) {
                logger.debug(format("[%s]:%s", SIGNALING, tuple.toString()));
            }
            spoutOutputCollector.emit(SIGNALING, tuple);
            logger.info(String.format("spout sent:%s,%s", tuple.get(0), tuple.get(1)));
        }
//        else {
//            logger.info(String.format("spout polls null tuple, sleep(10);"));
//            Utils.sleep(10);
//        }
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
        logger.debug("successfully ack(): " + msgId.toString());
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        logger.error("fail(): " + msgId.toString());
    }
}
