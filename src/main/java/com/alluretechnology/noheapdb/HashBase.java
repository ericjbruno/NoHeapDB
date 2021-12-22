package com.alluretechnology.noheapdb;

/**
 * @author ebruno
 */
public interface HashBase {
    public boolean put(String k, Integer s, Long o);
    public Long get(String k);
    public void remove(String k);
    public int getCollisions();
    public int getLoad();
    public void outputStats();
    public void reset();
}
