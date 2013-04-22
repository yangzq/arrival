package arrival.storm;

import arrival.util.KbUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-17
 * Time: 上午10:57
 */
public class PreconditionBolt extends BaseBasicBolt {
    private static Logger logger = LoggerFactory.getLogger(PreconditionBolt.class);
    public static final String PRECONDITION = "preconditionStream";
    public static final String UPDATETIME = "updateTimeStream";
    private BasicOutputCollector outputCollector;
    private long lastSignalTime = 0L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        this.outputCollector = basicOutputCollector;
        String imsi = tuple.getString(0);
        String eventType = tuple.getString(1);
        Long time = tuple.getLong(2);
        String lac = tuple.getString(3);
        String cell = tuple.getString(4);
        System.out.println(String.format("[%s]%s,%s,%s,%s,%s", PRECONDITION, imsi, eventType, time, lac, cell));
        logger.info(String.format("[%s]%s,%s,%s,%s,%s", PRECONDITION, imsi, eventType, time, lac, cell));
        if (logger.isDebugEnabled()){
            logger.debug(String.format("[%s]%s,%s,%s,%s,%s", PRECONDITION, imsi, eventType, time, lac, cell));
        }

        boolean matchARPU = KbUtils.getInstance().checkARPU(imsi);
        if (matchARPU) {
            basicOutputCollector.emit(PRECONDITION, new Values(imsi, eventType, time, lac, cell));
        } else {
            if (logger.isDebugEnabled()){
                logger.debug(String.format("ARPU not match:%s", imsi));
            }
        }

        if (time > lastSignalTime){
            basicOutputCollector.emit(UPDATETIME, new Values(time, imsi));
            lastSignalTime = time;
            logger.debug(String.format("[%s]%s", UPDATETIME, time));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PRECONDITION, new Fields("imsi", "eventType", "time", "lac", "cell"));
        outputFieldsDeclarer.declareStream(UPDATETIME, new Fields("time","imsi"));
    }
}
