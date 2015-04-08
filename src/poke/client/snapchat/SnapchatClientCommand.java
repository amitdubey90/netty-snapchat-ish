package poke.client.snapchat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import poke.comm.App.ClusterMessage;
import poke.comm.App.Header;
import poke.comm.App.Payload;
import poke.comm.App.Ping;
import poke.comm.App.Request;

import com.google.protobuf.ByteString;

public class SnapchatClientCommand {
	String host;
	int port;
	SnapchatCommunication comm;

	public SnapchatClientCommand(String host, int port) {
		this.host = host;
		this.port = port;
		comm = new SnapchatCommunication(host, port);
	}

	public void poke(String tag, int num) {
		// init start
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

		ClusterMessage.Builder f = ClusterMessage.newBuilder();
		try {
			file = new File(filePath);
			f.setMsgImageName(file.getName());
			f.setSenderUserName("sender");
			f.setReceiverUserName("receiver");
			f.setMsgText("hello");
			// FileInputStream fs = new FileInputStream(file);
			byte[] bytes = Files.readAllBytes(Paths.get(filePath));
			System.out.println("Sending file of length" + bytes.length);
			f.setMsgImageBits(ByteString.copyFrom(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		}
		f.setMsgId(UUID.randomUUID().toString());

		Request.Builder r = Request.newBuilder();
		Payload.Builder p = Payload.newBuilder();
		p.setClusterMessage(f.build());
		r.setBody(p.build());

		Header.Builder h = Header.newBuilder();
		h.setOriginator(1000);
		h.setTag("Image");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(Header.Routing.JOBS);
		r.setHeader(h.build());
		try {
			comm.enquesRequest(r.build());
			System.out.println("Message enqued");
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
					sc.sendImage("/Users/amit/Desktop/Smiley.svg");
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
