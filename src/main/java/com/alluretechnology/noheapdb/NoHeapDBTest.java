package com.alluretechnology.noheapdb;

import com.alluretechnology.noheapdb.DataStore;
import com.alluretechnology.noheapdb.NoHeapDB;

/**
 * @author ebruno
 */
public class NoHeapDBTest {
    int stringRecordCount = 80_000;//2_000_000_000;//1_000_000_000;// 1_500_000_000;
    
    public static void main(String[] args) {
        NoHeapDBTest db = new NoHeapDBTest();
    }
    
    public NoHeapDBTest() {
        int count = 1;
        
        NoHeapDB db = new NoHeapDB();
        try {
            for ( int i = 0; i < count; i++ ) {
                db.createStore( "TestDB"+i, 
                                DataStore.Storage.IN_MEMORY, 
                                1024); //  Megabyte

                testStringsSave(db, "TestDB"+i);
                testStringsLoad(db, "TestDB"+i, true);

                db.deleteStore("TestDB"+i);
            }
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    public void testStringsSave(NoHeapDB db, String storeName) {
        System.out.println("*** STRING PERSIST TEST ****\n");

        String largeData = "x".repeat(50_000);
        double start = System.currentTimeMillis();
        int i = 0;
        for ( i = 0; i < stringRecordCount; i++ ) {
            String key = "Eric"+i;
            db.putString(storeName, key, largeData+i);//"Bruno"+i);
            if ( i % 500_000 == 0 ) {
                System.out.println("Created " + i + " records so far");
            }
        }        

        double end = System.currentTimeMillis();
        double seconds = Math.max((end/1000 - start/1000), 1);
        long perSec = (long)((double)i / (double)seconds);
        long empty = db.getEmptyCount(storeName);
        long active = db.getRecordCount(storeName);

        System.out.println("Seconds: "+ seconds +", Msgs per second written: " + perSec);
        System.out.println("Collisions: " + db.getCollisions(storeName) );
        db.outputStats(storeName);
        System.out.println("Empty records: " + empty);
        System.out.println("Active records: " + active);
    }

    public void testStringsLoad(NoHeapDB db, String storeName, boolean displayError) {
        System.out.println("*** STRING LOAD TEST ****\n");
    
        int failedGets = 0;
		double start = System.currentTimeMillis();
        int i = 0;
        String key = "";
        String val = "";
        for ( i = 0; i < stringRecordCount; i++ ) {
            key = "Eric"+i;
            val = db.getString(storeName, key);
            if ( val == null ) {
                failedGets++;
                if ( displayError ) {
                    System.out.println("Object is null for item " + i);
                    System.exit(i);
                }
            }
        }

        double end = System.currentTimeMillis();
        double seconds = Math.max((end/1000 - start/1000), 1);
        long perSec = (long)((double)i / (double)seconds);

        System.out.println("Seconds: "+ seconds +", Msgs per second read: " + perSec);
        System.out.println("Failed gets: "+ failedGets);
        System.out.println("Object storage time: " + db.getObjectStorageTime(storeName));
        System.out.println("Object retrieval time: " + db.getObjectRetrievalTime(storeName));
    }
}
