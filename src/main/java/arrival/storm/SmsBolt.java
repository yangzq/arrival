package arrival.storm;

import arrival.util.TimeUtil;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-24
 * Time: 下午7:07
 */
public class SmsBolt extends BaseBasicBolt{
    private Logger logger = LoggerFactory.getLogger(SmsBolt.class);
    private Logger countLogger = LoggerFactory.getLogger("arrival.count");

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String imsi = input.getString(1);
        long time = input.getLong(0);
        logger.info(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
        countLogger.info(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
