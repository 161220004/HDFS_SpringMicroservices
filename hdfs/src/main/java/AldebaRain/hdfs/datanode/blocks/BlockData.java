package AldebaRain.hdfs.datanode.blocks;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class BlockData implements Serializable {

	private static final long serialVersionUID = -1666644079279045084L;

	@Id 
	@GeneratedValue
	private Integer id;

	/** 文件名 */
	private String filename;

	/** 该文件的块数量 */
	private int blockNum;
	
	/** 该块id */
	private Integer blockId;
	
	/** 块内容 */
	private byte[] data;

	public BlockData() {}

	public BlockData(String filename, int blockNum, Integer blockId, byte[] data) {
		this.filename = filename;
		this.blockId = blockId;
		this.blockNum = blockNum;
		this.data = data;
	}

	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public int getBlockNum() {
		return blockNum;
	}
	public void setBlockNum(int blockNum) {
		this.blockNum = blockNum;
	}
	public Integer getBlockId() {
		return blockId;
	}
	public void setBlockId(Integer blockId) {
		this.blockId = blockId;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	
}
