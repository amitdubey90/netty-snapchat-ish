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
import javax.activation.MimetypesFileTypeMap;

import com.google.protobuf.ByteString;

public class SnapchatClientCommand {
	String host;
	int port;
	SnapchatCommunication comm;
	private int  clientId;
	
	public SnapchatClientCommand(String host, int port, int clientId) {
		this.host = host;
		this.port = port;
		this.clientId=clientId;   
		//clientId=new Random().nextInt(100);
		comm = new SnapchatCommunication(host, port,clientId);
		registerClient();
	}

	public void registerClient(){
		//header message for request
		Header.Builder header = Header.newBuilder();
		header.setRoutingId(Routing.REGISTER);
		header.setOriginator(1000);
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

	public boolean sendImage(String filePath) {
		File file = null;
		boolean fileSupported=true;
		long filesize=0;
		System.out.println("Sending msg");
		//create client message for payload
		ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
		try {
			file = new File(filePath);
			//checking the image size and whether the  file is an image
			 filesize=file.length();
			 MimetypesFileTypeMap mtftp = new MimetypesFileTypeMap();
			 mtftp.addMimeTypes("image png tif jpg jpeg bmp");
			 String mimetype=  mtftp.getContentType(file);
			        
			        
		    if(mimetype.substring(0,5).equalsIgnoreCase("image")  && filesize<(15*1024*1024)){
			  clientMessage.setMsgImageName(file.getName());
			  clientMessage.setSenderUserName(clientId);
			  //f.setReceiverUserName("receiver");
			  clientMessage.setMsgText("hello");
			  clientMessage.setMessageType(MessageType.REQUEST);
			  clientMessage.setIsClient(true);
			  clientMessage.setBroadcastInternal(true);
			  // FileInputStream fs = new FileInputStream(file);
			  byte[] bytes = Files.readAllBytes(Paths.get(filePath));
			  //System.out.println("Sending file of length" + bytes.length);
			  clientMessage.setMsgImageBits(ByteString.copyFrom(bytes));
	        }
	        else{
	        	
	        	//perform the operations you want to, when file is not supported
	           System.out.println("File not supported");
	           
	        }
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
        if(fileSupported)return true;
        else return false;
	}

	public static void main(String[] args) {

		SnapchatClientCommand sc = new SnapchatClientCommand("localhost", 5570,11);
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
					if(sc.sendImage("/Users/dhavalkolapkar/Pictures/1.jpg"))
						System.out.println("file supported");
					else System.out.println("file not supported");
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
