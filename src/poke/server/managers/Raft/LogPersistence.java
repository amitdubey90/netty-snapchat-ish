package poke.server.managers.Raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import redis.clients.jedis.Jedis;

//import redis.clients.jedis.PipelineBlock;


public class LogPersistence extends Thread{
	
	int lastWrittenLogIndex;
	int buffer = 5;
	Jedis j;
	
	public LogPersistence(){
		lastWrittenLogIndex = 0;
		j = new Jedis("localhost", 6379);
	}
	   
	//Connecting to Redis server on localhost

	
	
	public Jedis getRedisConnection(){
		return j;
	}
	
	
	public void persistLog(LogEntry log){
		String temp = "";
		temp = "PrevLogTerm:" + log.prevLogTerm + "  PrevLogIndex:" + log.prevLogIndex + "  LogTerm:"+log.term+ "  LogIndex:"+log.logIndex +"  ClientRequest:"+log.logData;
		j.set(Integer.toString(log.logIndex), temp);
		lastWrittenLogIndex = log.logIndex;
	}
	
	@Override
	public void run() {
		
		while(true){
			
			try {
				Thread.sleep(1000);
				
				if(LogManager.commitIndex - lastWrittenLogIndex > buffer){
					
					LogManager lm = new LogManager();
					
					List<LogEntry> l  = new ArrayList<LogEntry>();
					l = lm.getLogsForPersistence(LogManager.commitIndex - lastWrittenLogIndex);
					Iterator<LogEntry> it = l.iterator();
					
					while(it.hasNext()){
						persistLog(it.next());
					}
				}
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
}
