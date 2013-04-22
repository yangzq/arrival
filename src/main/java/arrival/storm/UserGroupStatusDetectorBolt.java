package arrival.storm;

import arrival.util.TimeUtil;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arrival.util.Accout;
import arrival.util.UserGroup;

import java.io.IOException;
import java.util.Map;

/**
 * 接收所有的信令
 */
public class UserGroupStatusDetectorBolt extends BaseBasicBolt implements UserGroup.Listener {
    private static Logger logger = LoggerFactory.getLogger(UserGroupStatusDetectorBolt.class);
    private UserGroup userGroup = new UserGroup(this);
    private BasicOutputCollector outputcollector;
    public static final String DETECTORSTREAM = "detectorStream";

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.outputcollector = collector;
        String sourceStreamId = input.getSourceStreamId();
        if (PreconditionBolt.PRECONDITION.equals(sourceStreamId)) {
            String imsi = input.getString(0);
            String eventType = input.getString(1);
            long time = input.getLong(2);
            String lac = input.getString(3);
            String cell = input.getString(4);
            try {
                userGroup.onSignal(time, eventType, imsi, lac, cell);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else if (PreconditionBolt.UPDATETIME.equals(sourceStreamId)) {
            userGroup.updateGlobleTime(input.getLong(0), input.getString(1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(DETECTORSTREAM, new Fields("time", "imsi", "status")); //用户有三种状态:1：到港客户(arrival),2:工作人员(worker),3:什么都不是(normal)
    }

    @Override
    public void onAddArrival(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Arrival) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "arrival"));
        System.out.println(String.format("+a:%s %s/%s", imsi, userTime, TimeUtil.getTime(userTime)));
    }

    @Override
    public void onAddWorker(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Worker) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "worker"));
        System.out.println(String.format("-w:%s %s/%s", imsi, userTime, TimeUtil.getTime(userTime)));
    }

    @Override
    public void onAddNormal(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Normal) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "normal"));
        System.out.println(String.format("-n:%s %s/%s", imsi, userTime, TimeUtil.getTime(userTime)));
    }

    @Override
    public void cleanup() {
        userGroup.close();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.userGroup.init();
    }
}
