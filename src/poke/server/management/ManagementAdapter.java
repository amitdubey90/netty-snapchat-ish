package poke.server.management;

import java.util.Collection;

import io.netty.channel.Channel;
import poke.core.Mgmt.Management;
import poke.server.managers.ConnectionManager;
import poke.server.managers.ConnectionManager.connectionState;

public class ManagementAdapter {
	
	public synchronized static void sendToNode(Management mgmt, int dest){
		
		if (mgmt == null)
			return;
		
		Channel ch = ConnectionManager.getConnection(dest, connectionState.MGMT);
		
		if(ch!=null){
			ManagementQueue.enqueueResponse(mgmt, ch);
		}
	}
	
	public synchronized static void flushBroadcast(Management mgmt){
		
		Collection<Channel> ch = ConnectionManager.getMgmtConnections();
		
		if(ch == null)
			return;
		
		for(Channel c: ch)
			ManagementQueue.enqueueResponse(mgmt, c);
		
	}

}
