package poke.server.conf;

import java.util.ArrayList;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

public class ClusterConfList {

	ArrayList<ClusterConf> cList;

	public ArrayList<ClusterConf> getAllClusterConfs() {
		return cList;
	}

	public ClusterConf getClusterConfByID(int clusterID) {
		return null;
	}

	@XmlRootElement(name = "clusterConf")
	@XmlAccessorType(XmlAccessType.FIELD)
	public class ClusterConf {
		private int clusterId = -1;
		private String clusterName;
		private TreeMap<Integer, NodeDesc> clusterNodes;

		/**
		 * @return the clusterId
		 */
		public int getClusterId() {
			return clusterId;
		}

		/**
		 * @param clusterId
		 *            the clusterId to set
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
		 * @param clusterName
		 *            the clusterName to set
		 */
		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}

		/**
		 * @return the clusterNodes
		 */
		public TreeMap<Integer, NodeDesc> getClusterNodes() {
			return clusterNodes;
		}

		/**
		 * @param clusterNodes
		 *            the clusterNodes to set
		 */
		public void setClusterNodes(TreeMap<Integer, NodeDesc> clusterNodes) {
			this.clusterNodes = clusterNodes;
		}

	}
}
