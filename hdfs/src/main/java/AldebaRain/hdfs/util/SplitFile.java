package AldebaRain.hdfs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.Main;

import java.util.ArrayList;
import java.util.List;

public class SplitFile {
	
	/** 文件名 */
	private String filename;
	
	/** 文件块数量 */
	private int blockNum;
	
	/** 文件 */
	private File file;
	
	public SplitFile(String filename) {
		this.filename = filename;
		file = new File(this.filename);
		blockNum = (int) Math.ceil(file.length() / (double) Main.BlockSize);
	}

	/** 分割文件为block */
	public List<Block> split() {
		List<Block> blocks = new ArrayList<>();
		// 读取并分割文件
		RandomAccessFile rfile;
		try {
			rfile = new RandomAccessFile(file, "r"); // 只读
			// 开始分割（按线程，一个线程一个block）
			for (int blockId = 0; blockId < blockNum; blockId++) {
				Block block = new Block(filename, blockNum, blockId);
				SplitRunnable runnable = new SplitRunnable(blockId * Main.BlockSize, rfile, block);
				runnable.start();
				blocks.add(block);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return blocks;
	}
	
	public int getBlockNum() {
		return blockNum;
	}

}
