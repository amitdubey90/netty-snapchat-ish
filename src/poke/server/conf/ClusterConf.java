package poke.server.conf;

import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "clusterConf")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterConf {
	private int clusterId = -1;
	private String clusterName;
	private ClusterNodes clusterNodes;

	@XmlRootElement(name = "adjacent")
	@XmlAccessorType(XmlAccessType.FIELD)
	public static final class ClusterNodes {
		private TreeMap<Integer, NodeDesc> clusterNodes;
	
		public NodeDesc getNode(String name) {
			return clusterNodes.get(name);
		}
	
		public void add(NodeDesc node) {
			if (node == null)
				return;
			else if (clusterNodes == null)
				clusterNodes = new TreeMap<Integer, NodeDesc>();
	
			clusterNodes.put(node.getNodeId(), node);
		}
	
		public TreeMap<Integer, NodeDesc> getAdjacentNodes() {
			return clusterNodes;
		}
	
		public void setAdjacentNodes(TreeMap<Integer, NodeDesc> nearest) {
			this.clusterNodes = nearest;
		}
	}

	/**
	 * @return the clusterId
	 */
	public int getClusterId() {
		return clusterId;
	}

	/**
	 * @param clusterId the clusterId to set
	 */
	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}

	/**
	 * @return the clusterName
	 */
	public String getClusterName() {
		return clusterName;
	}

	/**
	 * @param clusterName the clusterName to set
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * @return the clusterNodes
	 */
	public ClusterNodes getClusterNodes() {
		return clusterNodes;
	}

	/**
	 * @param clusterNodes the clusterNodes to set
	 */
	public void setClusterNodes(ClusterNodes clusterNodes) {
		this.clusterNodes = clusterNodes;
	}
	
}
