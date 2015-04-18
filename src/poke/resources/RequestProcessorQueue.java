package poke.resources;

import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.ClientMessage;
import poke.comm.App.ClusterMessage;
import poke.comm.App.Header;
import poke.comm.App.Payload;
import poke.comm.App.Request;
import poke.server.managers.ConnectionManager;
import poke.server.managers.Raft.LogEntry;

public class RequestProcessorQueue {
	protected static Logger logger = LoggerFactory
			.getLogger("RequestProcessor");
	// public static HashMap<String,Request> reqQueue = new
	// HashMap<String,Request>();
	private static LinkedBlockingDeque<poke.comm.App.Request> outbound = new LinkedBlockingDeque<poke.comm.App.Request>();
	private static int senderUserName;

	/*
	 * private static Request checkMsgReceived(String logData){
	 * 
	 * List<String> items = Arrays.asList(logData.split("\\s*,\\s*")); String
	 * msgId=items.get(1); logger.info("Msg Id is: "+msgId); return
	 * reqQueue.get(msgId); }
	 */

	public static void init() {
		logger.info("Request processor initialized");
		new Thread(new RequestProcessorWorker()).start();
	}

	public static void processRequest(LogEntry logEntry) {

		logger.info("Using Request processor");
		poke.core.Mgmt.ClientMessage logData = logEntry.getLogData();
		logger.info("LogData is: " + logData);
		Request req = convertToClientRequest(logData);
		if (req != null) {
			senderUserName = req.getBody().getClientMessage()
					.getSenderUserName();
			logger.info("Request to be broadcast to clients except client id: "
					+ senderUserName);
		}
		// broadcast to clients

		ConnectionManager.broadcastToClients(req, senderUserName);
	}

	private static Request convertToClientRequest(
			poke.core.Mgmt.ClientMessage logData) {
		poke.comm.App.ClientMessage.Builder clientMsg = poke.comm.App.ClientMessage
				.newBuilder();

		clientMsg.setSenderUserName(logData.getSenderUserName());
		clientMsg.setReceiverUserName(logData.getReceiverUserName());

		clientMsg.setIsClient(logData.getIsClient());
		// logData.setBroadcastInternal(false);

		clientMsg.setMsgId(logData.getMsgId());
		clientMsg.setMsgImageName(logData.getMsgImageName());
		clientMsg.setMsgImageBits(logData.getMsgImageBits());
		clientMsg.setMsgText(logData.getMsgText());

		Header.Builder head = Header.newBuilder();
		head.setOriginator(1000);
		head.setTag("Image");
		head.setTime(System.currentTimeMillis());
		head.setRoutingId(Header.Routing.JOBS);

		Payload.Builder payload = Payload.newBuilder();
		payload.setClientMessage(clientMsg);

		// add body to request
		Request.Builder req = Request.newBuilder();
		req.setBody(payload);
		req.setHeader(head);

		return req.build();
	}
	
	public static Request convertClientToCluster(Request request) {

		ClientMessage requestClienteMsg = request.getBody().getClientMessage();
		
		poke.comm.App.ClientMessage.Builder respClientMessage = poke.comm.App.ClientMessage
				.newBuilder();

		respClientMessage.setSenderUserName(requestClienteMsg.getSenderUserName());
		respClientMessage.setReceiverUserName(requestClienteMsg.getReceiverUserName());

		respClientMessage.setIsClient(requestClienteMsg.getIsClient());
		// logData.setBroadcastInternal(false);

		respClientMessage.setMsgId(requestClienteMsg.getMsgId());
		respClientMessage.setMsgImageName(requestClienteMsg.getMsgImageName());
		respClientMessage.setMsgImageBits(requestClienteMsg.getMsgImageBits());
		respClientMessage.setMsgText(requestClienteMsg.getMsgText());

		Header.Builder head = Header.newBuilder();
		head.setOriginator(1000);
		head.setTag("Image");
		head.setTime(System.currentTimeMillis());
		head.setRoutingId(Header.Routing.JOBS);

		Payload.Builder payload = Payload.newBuilder();
		
		ClusterMessage.Builder cm = ClusterMessage.newBuilder(); 
		cm.setClientMessage(respClientMessage);
		payload.setClusterMessage(cm);
		
		// add body to request
		Request.Builder req = Request.newBuilder();
		req.setBody(payload);
		req.setHeader(head);
		
		return req.build();
	}

	public static void enqueRequest(LogEntry log) {
		try {
			poke.core.Mgmt.ClientMessage clientMessage = log.getLogData();
			poke.comm.App.Request request = convertToClientRequest(clientMessage);
			// logger.info("Request enqued to be delivered!!!!!!"+
			// request.toString());
			outbound.put(request);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	public static class RequestProcessorWorker extends Thread {
		boolean forever = true;

		@Override
		public void run() {

			while (true) {
				if (!forever)
					break;

				try {
					logger.info("Request processor waiting ....");
					// block until a message is enqueued
					poke.comm.App.Request request = outbound.take();

					// if (logger.isDebugEnabled())
					logger.info("Attempting to send request to client!");
					if (request != null) {
						logger.info("Sending request to client! - "
								+ request.getBody().getClientMessage()
										.getSenderUserName());
						ConnectionManager.broadcastToClients(request, request
								.getBody().getClientMessage()
								.getSenderUserName());
						if (request.getBody().getClientMessage().getIsClient()) {
							Request req = convertClientToCluster(request);
							logger.info("Sending request to other clusters!");
							ConnectionManager.broadcast(req);
						}

					} else {
						// logger.info("channel to node " +
						// msg.req.getHeader().getToNode() +
						// " is not writable");
						outbound.put(request);
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					// logger.error("Unexpected management communcation failure",
					// e);
					break;
				}
			}

			if (!forever) {
				logger.info("management outbound queue closing");
			}

		}
	}

}
