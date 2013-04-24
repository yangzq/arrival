package arrival.storm;

import arrival.util.AccountSnapshot;
import arrival.util.Accout;
import arrival.util.EditLog;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class EditLogTest {
    @Test
    public void testWriteCountAndReadCount() throws Exception {
        EditLog<AccountSnapshot> editlog = new EditLog<AccountSnapshot>(new File("c:\\log\\"), AccountSnapshot.class);
        AccountSnapshot accountSnapshot = new AccountSnapshot("arrival1",1213123L,true,true,1L,1L,true,new long[30], Accout.Status.Arrival);
        editlog.append(accountSnapshot);
        accountSnapshot = new AccountSnapshot("arrival1",1213124L,true,true,1L,1L,true,new long[30], Accout.Status.Arrival);
        editlog.append(accountSnapshot);
        editlog.close();
        editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
            @Override
            public boolean on(AccountSnapshot record) {
                System.out.println("--------------------------------");
                System.out.println("imsi:"+record.getImsi());
                System.out.println("time:"+record.getTime());
                return true;
            }
        });

    }


}
