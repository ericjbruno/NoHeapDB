package com.alluretechnology.noheapdb;

import java.io.File;
import java.util.HashMap;

/**
 *
 * @author ebruno
 */
public class NoHeapDB {
    protected final static int MEGABYTE  = 1024 * 1024;
    protected final static int DEFAULT_STORE_SIZE = MEGABYTE * 100;
    
    HashMap<String, DataStore> stores = new HashMap<>();
    
    String homeDirectory = 
            System.getProperty("user.home")+
            File.separator+"JavaOffHeap";
    
    public NoHeapDB() {
    }
    
    public NoHeapDB(String homeDirectory) {
        this.homeDirectory = homeDirectory;
    }
	
	public boolean createStore( String name ) throws Exception {
        return createStore(name, 
                            DataStore.Storage.IN_MEMORY, 
                            100);
    }
    
	public boolean createStore( String name, 
                                DataStore.Storage storageType ) 
                                    throws Exception {
        return createStore( name, 
                            storageType, 
                            100);
    }

	public boolean createStore( String name, 
                                DataStore.Storage storageType,
                                int size ) throws Exception {
        if ( size > Integer.MAX_VALUE ) {
            throw new Exception("Database size exceeds " + Integer.MAX_VALUE );
        }
        
        NoHeapDBStore nohdb = new 
            NoHeapDBStore( homeDirectory, "joh-" + name + "-", 
                            storageType, 
                            size * MEGABYTE, true);
        
        stores.put(name, nohdb);
        
        return true;
    }

    public DataStore getStore(String storeName) {
        return this.stores.get(storeName);
    }

    public boolean deleteStore(String storeName) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            // Delete the store here
            store.delete();
            if ( stores.remove(storeName) != null ) {
                return true;
            }
        }
        return false;
    }
	
    public boolean putString(String storeName, String key, String value) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putString(key, value);
        }
        
        return false;
    }
    
    public boolean putInteger(String storeName, String key, Integer value) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putInteger(key, value);
        }
        
        return false;
    }
    
    public boolean putShort(String storeName, String key, Short value){
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putShort(key, value);
        }
        
        return false;
    }
    
    public boolean putLong(String storeName, String key, Long value){
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putLong(key, value);
        }
        
        return false;
    }

    public boolean putFloat(String storeName, String key, Float value){
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putFloat(key, value);
        }
        
        return false;
    }

    public boolean putDouble(String storeName, String key, Double value) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putDouble(key, value);
        }
        
        return false;
    }

    public boolean putChar(String storeName, String key, char value) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putChar(key, value);
        }
        
        return false;
    }

    public boolean putObject(String storeName, String key, Object value) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.putObject(key, value);
        }
        
        return false;        
    }
    
    public String getString(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getString(key);
        }
        
        return null;
    }
    
    public Integer getInteger(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getInteger(key);
        }
        
        return null;
    }

    public Short getShort(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getShort(key);
        }
        
        return null;
    }
    
    public Long getLong(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getLong(key);
        }
        
        return null;
    }
    
    public Float getFloat(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getFloat(key);
        }
        
        return null;
    }
    
    public Double getDouble(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getDouble(key);
        }
        
        return null;
    }
    
    public char getChar(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getChar(key);
        }
        
        return (char)0;
    }
    
    public Object getObject(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.getObject(key);
        }
        
        return null;
    }
    
	public boolean remove(String storeName, String key) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.remove(key);
        }
        
        return false;
    }
    
    public Object iterateStart(String storeName) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.iterateStart();
        }

        return null;
    }

    public Object iterateNext(String storeName) {
        DataStore store = this.stores.get(storeName);
        if ( store != null ) {
            return store.iterateNext();
        }

        return null;
    }
    

    public int getCollisions(String storeName) {
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        return niop.getCollisions();
    } 
    public int getIndexLoad(String storeName) {
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        return niop.getIndexLoad();
    }
    public long getObjectRetrievalTime(String storeName) { 
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        return niop.getObjectRetrievalTime(); 
    }
    public long getObjectStorageTime(String storeName) { 
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        return niop.getObjectStorageTime(); 
    }
    public void outputStats(String storeName) {
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        niop.outputStats();
    }
    public long getRecordCount(String storeName) {
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
		return niop.getRecordCount();
    }

    public long getEmptyCount(String storeName) {
        NoHeapDBStore niop = (NoHeapDBStore)stores.get(storeName);
        return niop.getEmptyCount();
    }
}
