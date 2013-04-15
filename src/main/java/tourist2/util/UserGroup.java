package tourist2.util;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.LazyMap;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 一组用户，其中，每个用户为一个User对象，当用户状态发生变更时，会发出通知给listener
 * 用户状态有三种：Worker，Tourist，Normal
 */
public class UserGroup implements Serializable{


  private Listener listener;
  private final Map<String,User> detectors = new HashMap<String, User>();
//  private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new UserTransformer());

  private class UserTransformer implements Transformer, Serializable {
    @Override
    public Object transform(final Object input) {
      try {
        return new User((String) input, listener);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public UserGroup(Listener listener) {
    this.listener = listener;
  }

  public void onSignal(long time, String imsi, String loc, String cell) throws IOException {
    User user =  detectors.get(imsi);
    if(user ==null){
        synchronized (detectors){
            user =  detectors.get(imsi);
            if(user==null){
                user = new User(imsi, listener);
                detectors.put(imsi,user);
            }
        }
    }
    user.onSignal(time, loc, cell);
  }

  public void updateGlobleTime(Long globalTime) {
    for (Object o : detectors.values()) {
      if (o instanceof User) {
        ((User) o).updateGlobleTime(globalTime);
      }
    }
  }

  public static interface Listener {
    void onAddTourist(long userTime, String imsi, Accout.Status preStatus);

    void onAddWorker(long userTime, String imsi, Accout.Status preStatus);

    void onAddNormal(long userTime, String imsi, Accout.Status preStatus);
  }
}
