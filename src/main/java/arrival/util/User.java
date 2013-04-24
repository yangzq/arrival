package arrival.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 一个用户，一个账户
 * 每个账户都有自己状态(Worker,Arrival,Normal),每当账户的状态发生变更时，会通知用户。
 * 用户合并两个账户的状态，如果发现合并后的状态变更了，则发出通知给用户组的Listener。
 */
public class User implements UserGroup.Listener {
    private static Logger logger = LoggerFactory.getLogger(User.class);
    private final String imsi;
    private final UserGroup.Listener listener;
    private Accout accout;
    private Accout.Status status = Accout.Status.Normal;

    public User(String imsi, UserGroup.Listener listener, EditLog<AccountSnapshot> editLog) throws IOException {
        this.imsi = imsi;
        this.listener = listener;
        accout = new Accout(imsi, this, editLog);
    }

    public void onSignal(long time, String eventType, String lac, String cell) {
        try {
            accout.onSignal(time, eventType, lac, cell);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onAddArrival(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Arrival) return;
        if (!accout.isWorker()) {
            listener.onAddArrival(userTime, imsi, status);
            status = Accout.Status.Arrival;
        }
    }

    @Override
    public void onAddWorker(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Worker) return;
        listener.onAddWorker(userTime, imsi, status);
        status = Accout.Status.Worker;
    }

    @Override
    public void onAddNormal(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Normal) return;
        if (!accout.isWorker()) {
            listener.onAddNormal(userTime, imsi, status);
            status = Accout.Status.Normal;
        }
    }

    @Override
    public void sendSms(long userTime, String imsi) {
        listener.sendSms(userTime, imsi);

    }

    public void updateGlobleTime(Long globalTime) {
        accout.updateGlobleTime(globalTime);
    }

    public String getImsi() {
        return imsi;
    }
}
