package AldebaRain.hdfs.namenode.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.HashMap;

public class SplitFile {
	
	/** 文件名 */
	private String fileName;
	
	/** 文件块数量 */
	private int blockNum;
	
	/** 文件块大小 */
	private int blockSize;
	
	/** 文件 */
	private File file;
	
	public SplitFile(String fileName, int blockSize) {
		this.fileName = fileName;
		this.blockSize = blockSize;
		file = new File(fileName);
		blockNum = (int) Math.ceil(file.length() / (double) blockSize);
	}

	/** 分割文件为block */
	public Map<Integer, byte[]> split() {
		Map<Integer, byte[]> blocks = new HashMap<>();
		// 读取并分割文件
		RandomAccessFile rfile;
		try {
			rfile = new RandomAccessFile(file, "r"); // 只读
			// 开始分割（按线程，一个线程一个block）
			for (int i = 0; i < blockNum; i++) {
				String blockName = getBlockName(i);
				byte[] block = new byte[blockSize];
				SplitRunnable runnable = new SplitRunnable(blockName, i * blockSize, rfile, block);
				runnable.start();
				blocks.put(i, block);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return blocks;
	}
	
	public int getBlockNum() {
		return blockNum;
	}

	public String getBlockName(int index) {
		return fileName + "." + String.valueOf(index);
	}

}
