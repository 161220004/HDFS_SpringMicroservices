package AldebaRain.hdfs;

import java.io.Serializable;

public class Block implements Serializable {

	private static final long serialVersionUID = 136425181921699732L;

	/** 文件名 */
	private String filename;

	/** 该文件的块数量 */
	private int blockNum;
	
	/** 该块id */
	private Integer blockId;
	
	/** 块内容 */
	private byte[] data;

	public Block() {}
	
	public Block(String filename, int blockNum, Integer blockId, byte[] data) {
		this.filename = filename;
		this.blockId = blockId;
		this.blockNum = blockNum;
		this.data = data;
	}

	public Block(String filename, int blockNum, Integer blockId) {
		this.filename = filename;
		this.blockId = blockId;
		this.blockNum = blockNum;
		this.data = null;
	}
	
	public String getFilename() {
		return filename;
	}

	public int getBlockNum() {
		return blockNum;
	}

	public Integer getBlockId() {
		return blockId;
	}

	public byte[] getData() {
		return data;
	}
	
}
