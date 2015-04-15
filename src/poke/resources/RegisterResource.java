package poke.resources;

import io.netty.channel.Channel;

import java.util.LinkedHashMap;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.ClientMessage;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.Header;
import poke.comm.App.Header.Routing;
import poke.comm.App.Payload;
import poke.comm.App.Request;
import poke.server.managers.ConnectionManager;
import poke.server.managers.ConnectionManager.connectionState;
import poke.server.resources.Resource;

public class RegisterResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("server");

	public static LinkedHashMap<Integer, Queue<Request>> clients = new LinkedHashMap<Integer, Queue<Request>>();
	
	public static void addClientToMap(int clientId, Queue<Request> Q){
		clients.put(clientId, Q);
	}
	
	public static void createClientQueue(int clientId){	
		Queue<Request> clientQueue = new LinkedBlockingQueue<Request>();
		addClientToMap(clientId, clientQueue);
	}
	
	public static Request getMessage(int clientId){
		Queue<Request> temp = clients.get(clientId);
		
		return (temp.remove());
	}
	
	public static void addMessageToQueue(int clientId, Request cr){
		if(clients.get(clientId) !=null){
			Queue<Request> temp = clients.get(clientId);
			temp.add(cr);
			clients.put(clientId,temp);
		}
		else{
			createClientQueue(clientId);
			Queue<Request> temp = clients.get(clientId);
			temp.add(cr);
			clients.put(clientId,temp);
		}
		
	}
	
	@Override
	public Request process(Request request,Channel ch) {
		//register client with connection manager
		int clientId = request.getBody().getClientMessage().getSenderUserName();
		ConnectionManager.addConnection(clientId, ch, connectionState.CLIENT);
		
		//Header
		Header.Builder header = Header.newBuilder();
		header.setRoutingId(Routing.REGISTER);
		header.setOriginator(1000);
		//cluster msg
		ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
		clientMessage.setMessageType(MessageType.SUCCESS);
		//empty Payload
		Payload.Builder body = Payload.newBuilder();
		body.setClientMessage(clientMessage);
		//Request
		Request.Builder reply = Request.newBuilder();
		reply.setBody(body);
		reply.setHeader(header);
		logger.info("Client "+clientId+" added to the network");
		return reply.build();
		// TODO Auto-generated method stub
		
	}

}
