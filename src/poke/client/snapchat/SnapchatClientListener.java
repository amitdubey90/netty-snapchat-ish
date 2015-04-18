package poke.client.snapchat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import poke.client.comm.CommListener;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.Header.Routing;
import poke.comm.App.Request;

import com.google.protobuf.ByteString;
import java.io.File;
import java.nio.file.Path;

public class SnapchatClientListener implements CommListener {

	private int clientId;
	private int counter;

	@Override
	public String getListenerID() {
		// TODO Auto-generated method stub

		return null;
	}

	public SnapchatClientListener(int clientId) {
		this.clientId = clientId;
	}

	@Override
	public void onMessage(Request request) {
		if (request.getHeader().getRoutingId() == Routing.REGISTER) {
			System.out.println("Client Registered!");
		} else if (request.getBody().getClientMessage().getMessageType() == MessageType.SUCCESS) {
			System.out.println("Message sent!");
		}
		if (request.getBody().hasClientMessage()) {
			System.out
					.println("Receiving msg from other client and msg type is: "
							+ request.getBody().getClientMessage()
									.getMessageType());
			ByteString bs = request.getBody().getClientMessage()
					.getMsgImageBits();
			byte[] bytes = bs.toByteArray();
			FileOutputStream fos;
			try {
				File f = new File("/Users/Amit/Pictures/SnapchatReceiver/"
						+ clientId);
				if (!f.exists()) {
					new File("/Users/Amit/Pictures/SnapchatReceiver/"
							+ clientId).mkdirs();
				}
				;
				StringBuilder sb = new StringBuilder(
						"/Users/Amit/Pictures/SnapchatReceiver/" + clientId+"/");
				sb.append("-"
						+ System.currentTimeMillis()
						+ ""
						+ request.getBody().getClientMessage()
								.getMsgImageName());
				fos = new FileOutputStream(sb.toString());
				fos.write(bytes, 0, bytes.length);
				fos.flush();
				fos.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("File received. Size - " + bytes.length);
		}
	}

}
