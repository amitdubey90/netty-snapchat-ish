package poke.resources;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.Request;
import poke.server.managers.ConnectionManager;
import poke.server.managers.Raft.LogEntry;

public class RequestProcessor {
	protected static Logger logger = LoggerFactory.getLogger("RequestProcessor");
	public static HashMap<String,Request> reqQueue = new HashMap<String,Request>();
	private static int senderUserName;
	
	private static  Request checkMsgReceived(String logData){
		
		List<String> items = Arrays.asList(logData.split("\\s*,\\s*"));
		String msgId=items.get(1);
		logger.info("Msg Id is: "+msgId);
		return  reqQueue.get(msgId);
	}
	
	public static void processRequest(LogEntry logEntry){
		
		logger.info("Using Request processor");
		String logData = logEntry.getLogData();
		logger.info("LogData is: "+logData);
		Request req=checkMsgReceived(logData);
		senderUserName=req.getBody().getClientMessage().getSenderUserName();
		logger.info("Req to be processed is: "+req);
		//broadcast to clients
		
		ConnectionManager.broadcastToClients(req, senderUserName);
	}
	
	
}
