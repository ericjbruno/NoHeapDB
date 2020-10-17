package com.alluretechnology.noheapdb;

/**
 * @author ebruno
 */
public interface HashBase {
    public boolean put(String k, Long v);
    public Long get(String k);
    public void remove(String k);
    public int getCollisions();
    public int getLoad();
    public void outputStats();
    public void reset();
}
