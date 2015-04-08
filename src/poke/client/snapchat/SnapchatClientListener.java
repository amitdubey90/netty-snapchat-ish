package poke.client.snapchat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import poke.client.comm.CommListener;
import poke.comm.App.Request;

import com.google.protobuf.ByteString;

public class SnapchatClientListener implements CommListener {

	@Override
	public String getListenerID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onMessage(Request request) {
		ByteString bs = request.getBody().getClusterMessage().getMsgImageBits();
		byte[] bytes = bs.toByteArray();
		FileOutputStream fos;
		try {
			StringBuilder sb = new StringBuilder("/Users/amit/Downloads/");
			sb.append(request.getBody().getClusterMessage().getMsgImageName());
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
