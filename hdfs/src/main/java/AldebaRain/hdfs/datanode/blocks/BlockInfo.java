package AldebaRain.hdfs.datanode.blocks;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class BlockInfo {

	@Id 
	@GeneratedValue
	private Integer id;
	
	private String filename;

	private String blockId;
	
	public BlockInfo(String filename, String blockId) {
		this.filename = filename;
		this.blockId = blockId;
	}
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getBlockId() {
		return blockId;
	}
	public void setBlockId(String blockId) {
		this.blockId = blockId;
	}
	
}
