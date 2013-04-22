package arrival.util;

import org.apache.commons.collections.Transformer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * 一组用户，其中，每个用户为一个User对象，当用户状态发生变更时，会发出通知给listener
 * 用户状态有三种：Worker、Arrival、Normal，分别对应机场工作人员、来港客户、普通用户；
 */
public class UserGroup implements Serializable {


    private Listener listener;
    private final Map<String, User> detectors = new HashMap<String, User>();
    private EditLog<AccountSnapshot> editLog = null;

    public void init() {
        this.editLog = new EditLog<AccountSnapshot>(new File(System.getProperty("java.io.tmpdir"), "UserGroup@" + this.hashCode() + new Random().nextInt(1000)), AccountSnapshot.class);

    }

//  private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new UserTransformer());

    private class UserTransformer implements Transformer, Serializable {
        @Override
        public Object transform(final Object input) {
            try {
                return new User((String) input, listener, editLog);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public UserGroup(Listener listener) {
        this.listener = listener;
    }

    public void onSignal(long time, String eventType, String imsi, String lac, String cell) throws IOException {
        User user = detectors.get(imsi);
        if (user == null) {
            synchronized (detectors) {
                user = detectors.get(imsi);
                if (user == null) {
                    user = new User(imsi, listener, editLog);
                    detectors.put(imsi, user);
                }
            }
        }
        user.onSignal(time, eventType, lac, cell);
    }

    public void updateGlobleTime(Long globalTime, String imsi) {
//        for (Object o : detectors.values()) {
//            if (o instanceof User) {
//                ((User) o).updateGlobleTime(globalTime);
//            }
//        }
        Set<String> keys = detectors.keySet();
        for (Iterator it = keys.iterator(); it.hasNext();){
            String currImsi = (String)it.next();
            Object o = detectors.get(currImsi);
            if ((!currImsi.equals(imsi)) && (o != null)){
                ((User) o).updateGlobleTime(globalTime);
            }
        }
    }

    public void close() {
        this.editLog.close();
    }

    public static interface Listener {
        void onAddArrival(long userTime, String imsi, Accout.Status preStatus);
        void onAddWorker(long userTime, String imsi, Accout.Status preStatus);
        void onAddNormal(long userTime, String imsi, Accout.Status preStatus);
    }

}
