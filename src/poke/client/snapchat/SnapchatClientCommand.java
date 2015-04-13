package poke.client.snapchat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;

import poke.comm.App.ClientMessage;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.Header;
import poke.comm.App.Header.Routing;
import poke.comm.App.Payload;
import poke.comm.App.Ping;
import poke.comm.App.Request;

import com.google.protobuf.ByteString;

public class SnapchatClientCommand {
	String host;
	int port;
	SnapchatCommunication comm;
	private int  clientId;
	
	public SnapchatClientCommand(String host, int port) {
		this.host = host;
		this.port = port;
		clientId=new Random().nextInt(100);
		comm = new SnapchatCommunication(host, port);
		registerClient();
	}

	public void registerClient(){
		//header message for request
		Header.Builder header = Header.newBuilder();
		header.setRoutingId(Routing.REGISTER);
		header.setOriginator(1000);
		header.setIsClusterMsg(false);
		//client message for payload
		ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
		clientMessage.setSenderUserName(clientId);
	//payload for request
	Payload.Builder body = Payload.newBuilder();
	body.setClientMessage(clientMessage);	
	//Request 
	Request.Builder request = Request.newBuilder();
	request.setHeader(header);
	request.setBody(body);
	
	//send to server
	try {
		comm.enquesRequest(request.build());
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
	}
	
	public void poke(String tag, int num) {
		
		Ping.Builder f = Ping.newBuilder();
		f.setTag(tag);
		f.setNumber(num);

		// payload containing data
		Request.Builder r = Request.newBuilder();
		Payload.Builder p = Payload.newBuilder();
		p.setPing(f.build());
		r.setBody(p.build());

		// header with routing info
		Header.Builder h = Header.newBuilder();
		h.setOriginator(1000);
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(Header.Routing.PING);
		r.setHeader(h.build());

		Request req = r.build();
		try {
			comm.enquesRequest(req);
			System.out.println("Msg enqued");
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}

	}

	public void sendImage(String filePath) {
		File file = null;

		//create client message for payload
		ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
		try {
			file = new File(filePath);
			clientMessage.setMsgImageName(file.getName());
			clientMessage.setSenderUserName(clientId);
			//f.setReceiverUserName("receiver");
			clientMessage.setMsgText("hello");
			clientMessage.setMessageType(MessageType.REQUEST);
			// FileInputStream fs = new FileInputStream(file);
			byte[] bytes = Files.readAllBytes(Paths.get(filePath));
			//System.out.println("Sending file of length" + bytes.length);
			clientMessage.setMsgImageBits(ByteString.copyFrom(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		}
		clientMessage.setMsgId(UUID.randomUUID().toString());

		//create payload for request
		Payload.Builder body = Payload.newBuilder();
		body.setClientMessage(clientMessage.build());
		
		//header for request
				Header.Builder header = Header.newBuilder();
				header.setOriginator(1000);
				header.setTag("Image");
				header.setTime(System.currentTimeMillis());
				header.setRoutingId(Header.Routing.JOBS);
				header.setIsClusterMsg(false);
				
		//request
		Request.Builder request = Request.newBuilder();
		request.setHeader(header);
		request.setBody(body);
		
		try {
			comm.enquesRequest(request.build());
			//System.out.println("Message enqued");
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}

	}

	public static void main(String[] args) {

		SnapchatClientCommand sc = new SnapchatClientCommand("localhost", 5570);
		int option = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		while (true) {
			System.out.println("1. Poke");
			System.out.println("2. Send Image");

			try {
				option = Integer.parseInt(br.readLine());
				switch (option) {
				case 1:
					sc.poke("haha", 1);
					break;

				case 2:
					sc.sendImage("/Users/dhavalkolapkar/Pictures/1.jpg");
					break;
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
			}

		}

	}
}
