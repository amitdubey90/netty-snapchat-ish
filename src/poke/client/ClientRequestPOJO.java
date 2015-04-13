package poke.client;

import java.io.File;

public class ClientRequestPOJO implements java.io.Serializable{

   String msgId;
   int senderUserName;
   int receiverUserName;
   String msgText;
   String msgImageName;
   byte[] image;
   
   public ClientRequestPOJO(String msgId, int senderUserName,
		int receiverUserName, String msgText, String msgImageName, byte[] image) {
	super();
	this.msgId = msgId;
	this.senderUserName = senderUserName;
	this.receiverUserName = receiverUserName;
	this.msgText = msgText;
	this.msgImageName = msgImageName;
	this.image = image;
}

   
   
	  
//	boolean isClient;
//	boolean broadcastInternal;
//	
//	optional MessageType messageType = 7 [default = SUCCESS];
//	  enum MessageType
//	  {
//	    REQUEST = 0;
//	    SUCCESS = 1;
//
//	    }
	
}
