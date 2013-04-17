package tourist2.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 汇总并输出游客数
 */
public class TouristCountBolt extends BaseBasicBolt {
    private Logger countLogger = LoggerFactory.getLogger("tourist.count");
//    private AtomicInteger countTourist = new AtomicInteger();
//    private AtomicInteger countWorker = new AtomicInteger();
//    private OutputCollector outputCollector;
    private BasicOutputCollector outputCollector;
    private Set touristImsi =  new HashSet();
    private Set workerImsi =  new HashSet();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        this.outputCollector = collector;
        long time = tuple.getLong(0);
        String imsi = tuple.getString(1);
        String identity = tuple.getString(2);
        if (identity.equals("tourist")){
            touristImsi.add(imsi);
        } else {
            touristImsi.remove(imsi);
        }
        if (identity.equals("worker")){
            workerImsi.add(imsi);
        } else {
            workerImsi.remove(imsi);
        }
        try {
            System.out.println(String.format("tourist:%s,imsi:%s,signal time:%s/%s", touristImsi.size(), StringUtils.join(touristImsi.toArray(),","), getTime(time), time));
            System.out.println(String.format("worker:%s,imsi:%s,signal time:%s/%s", workerImsi.size(), StringUtils.join(workerImsi.toArray(),","), getTime(time), time));
            if (countLogger.isInfoEnabled()){
                countLogger.info(String.format("tourist:%s,imsi:%s,signal time:%s/%s", touristImsi.size(), StringUtils.join(touristImsi.toArray(),","), getTime(time), time));
                countLogger.info(String.format("worker:%s,imsi:%s,signal time:%s/%s", workerImsi.size(), StringUtils.join(workerImsi.toArray(),","), getTime(time), time));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

//        this.outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

}
