package poke.server.managers.Raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

//import redis.clients.jedis.PipelineBlock;


public class LogPersistence extends Thread{
	
	int lastWrittenLogIndex;
	int buffer = 5;
	static Jedis j;
	//protected static Logger logger = LoggerFactory.getLogger("Persistence");
	
	public LogPersistence(){
		
		j = new Jedis("localhost", 6379);
		j.connect();
		
		Boolean dbConnected = j.isConnected();
		
		if(dbConnected){
			
			//logger.info("DB connected: " + dbConnected);
			int index = LogPersistence.j.keys("*").size();
			
			if(index != 0){
			
				Set<String> set = j.keys("*");
				Iterator<String> iterator = set.iterator();
				
				Integer max = Integer.parseInt(iterator.next());
				
				while(iterator.hasNext()) {
			        Integer element = Integer.parseInt(iterator.next());
			        if(element > max)
			        	max = element;
			    }
				
				LogManager.currentLogIndex = max;
				LogManager.commitIndex = max;
				lastWrittenLogIndex = max;
			
			}
			else{
				LogManager.currentLogIndex = 0;
				LogManager.commitIndex = 0;
				lastWrittenLogIndex = 0;
				
			}
			
			//logger.info("currentLogIndex: " + LogManager.currentLogIndex + "commitIndex: " + LogManager.commitIndex);
			
		}
		
	}
	   
	//Connecting to Redis server on localhost

	
	
	public Jedis getRedisConnection(){
		return j;
	}
	
	
	public void persistLog(LogEntry log){
		//TODO improve storing mechanism
		String temp = "";
		temp = "PrevLogTerm:" + log.prevLogTerm + "  PrevLogIndex:" + log.prevLogIndex + "  LogTerm:"+log.term+ "  LogIndex:"+log.logIndex +"  ClientRequest:"+log.logData;
		j.set(Integer.toString(log.logIndex), temp);
		lastWrittenLogIndex = log.logIndex;
		LogManager.logs.remove(log.logIndex);
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
