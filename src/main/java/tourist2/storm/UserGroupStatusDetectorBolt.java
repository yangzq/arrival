package tourist2.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tourist2.util.Accout;
import tourist2.util.UserGroup;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 接收所有的信令
 */
public class UserGroupStatusDetectorBolt extends BaseBasicBolt implements UserGroup.Listener {
    private static Logger logger = LoggerFactory.getLogger(SignalingSpout.class);
    private UserGroup userGroup = new UserGroup(this);
    //    private OutputCollector collector;
    private BasicOutputCollector outputcollector;
    public static final String DETECTORSTREAM = "detectorStream";
//    private ThreadLocal<Tuple> tuple = new ThreadLocal<Tuple>();

//    final AtomicInteger count = new AtomicInteger();
//    final AtomicLong ss = new AtomicLong(System.currentTimeMillis());

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        if (count.incrementAndGet() % 100000 == 1) {
//            long en = System.currentTimeMillis();
//            logger.info("cost8:" + (en - ss.get()));
//            ss.set(en);
//        }
        this.outputcollector = collector;
        String sourceStreamId = input.getSourceStreamId();
        if (SignalingSpout.SIGNALING.equals(sourceStreamId)) {
            String imsi = input.getString(0);
            long time = input.getLong(1);
            String loc = input.getString(2);
            String cell = input.getString(3);
            try {
                userGroup.onSignal(time, imsi, loc, cell);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        }
//        else if (SignalingSpout.TIME.equals(sourceStreamId)) {
//            userGroup.updateGlobleTime(input.getLong(0));
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(DETECTORSTREAM, new Fields("time", "imsi", "status")); //用户有三种状态:1：游客(tourist),2:工作人员(worker),3:什么都不是(normal)
    }

    @Override
    public void onAddTourist(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Tourist) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "tourist"));
        System.out.println(String.format("+t:%s %s", imsi, userTime));
    }

    @Override
    public void onAddWorker(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Worker) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "worker"));
        System.out.println(String.format("-w:%s %s", imsi, userTime));
    }

    @Override
    public void onAddNormal(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Normal) return;
        this.outputcollector.emit(DETECTORSTREAM, new Values(userTime, imsi, "normal"));
        System.out.println(String.format("-n:%s %s", imsi, userTime));
    }

  @Override
  public void cleanup() {
    userGroup.close();    //To change body of overridden methods use File | Settings | File Templates.
  }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.userGroup.init();
    }
}
