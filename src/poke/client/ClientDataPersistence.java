package poke.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import redis.clients.jedis.Jedis;

public class ClientDataPersistence {

	static Jedis j;
	
	public ClientDataPersistence(){
		j = new Jedis("localhost", 6379);
	}
	
	
	
	public static ClientRequestPOJO makeObject(String msgId, int senderUserName,
			int receiverUserName, String msgText, String msgImageName, byte[] image){
		
		
		ClientRequestPOJO req = new ClientRequestPOJO(msgId,senderUserName,receiverUserName,msgText,msgImageName,image);
		return req;
	}
	
	public static String encodePOJO(ClientRequestPOJO cp){
		
		String serializedObject = "";

		 // serialize the object
		 try {
		     ByteArrayOutputStream bo = new ByteArrayOutputStream();
		     ObjectOutputStream so = new ObjectOutputStream(bo);
		     so.writeObject(cp);
		     so.flush();
		     serializedObject = bo.toString("ISO-8859-1");
		 } catch (Exception e) {
		     System.out.println(e);
		 }
		 
		 return serializedObject;

		
	}
	
	public ClientRequestPOJO decodePOJO(String str){
		// deserialize the object
		ClientRequestPOJO obj = null;
		try {
		     byte b[] = str.getBytes("ISO-8859-1");
		     ByteArrayInputStream bi = new ByteArrayInputStream(b);
		     ObjectInputStream si = new ObjectInputStream(bi);
		     obj = (ClientRequestPOJO) si.readObject();
		     
		     
		 } catch (Exception e) {
		     System.out.println(e);
		 }
		return obj;
	}
	
	public static void setInRedis(int msgId, String s){
		j.set(Integer.toString(msgId), s);
	
	}
	
	public static String getFromRedis(int msgId){
		return j.get(Integer.toString(msgId));
	}
	
	
//	public static void main(String[] args){
//		
//		
//		
//		String dummy = encodePOJO(makeObject());
//		System.out.println(dummy);
//		setInRedis(3, dummy);
//		setInRedis(4, dummy);
//		setInRedis(5, dummy);
//		
//		decodePOJO(getFromRedis(1));
//		
//	}
}