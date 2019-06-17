package AldebaRain.hdfs.datanode;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class DataNodeInfo {

	@Id 
	@GeneratedValue
	private Integer id;
	
	private String datanodeId;

	public String getDatanodeId() {
		return datanodeId;
	}

	public void setDatanodeId(String datanodeId) {
		this.datanodeId = datanodeId;
	}
	
}
