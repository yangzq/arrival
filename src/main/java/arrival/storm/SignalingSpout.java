package arrival.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import arrival.util.NioServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 * SignalingSpout
 */
public class SignalingSpout extends BaseRichSpout {
    public static final String SIGNALLING = "signalStream";
    private static Logger logger = LoggerFactory.getLogger(SignalingSpout.class);
    LinkedBlockingQueue<String> queue = null;
    NioServer nioServer = null;
    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIGNALLING, new Fields("imsi", "eventType", "time", "lac", "cell"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(10000);
        NioServer.Listener listener = new NioServer.Listener() {
            @Override
            public void messageReceived(String message) throws Exception {
                if (logger.isDebugEnabled()) {
                    logger.info(String.format("spout received:%s", message));
                }
                queue.put(message); // 往队列中添加信令时阻塞以保证数据不丢失
            }
        };
        nioServer = new NioServer(5003, listener);
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
            String[] columns = message.split(",");
            Values tuple = new Values(columns[0], columns[1], Long.parseLong(columns[2]), columns[3], columns[4]);
            if (logger.isDebugEnabled()) {
                logger.debug(format("[%s]:%s", SIGNALLING, tuple.toString()));
            }
            spoutOutputCollector.emit(SIGNALLING, tuple);
            if (logger.isDebugEnabled()) {
                logger.info(String.format("spout sent:%s,%s,%s,%s,%s", tuple.get(0), tuple.get(1), tuple.get(2), tuple.get(3), tuple.get(4)));
            }
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
        if (logger.isDebugEnabled()) {
            logger.debug("successfully ack(): " + msgId.toString());
        }
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        logger.error("fail(): " + msgId.toString());
    }
}
