package AldebaRain.hdfs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import AldebaRain.hdfs.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SplitFile {
	
	/** 文件名 */
	private String filename;
	
	/** 文件块大小 */
	private int blockSize;
	
	/** 文件块数量 */
	private int blockNum;
	
	/** 文件 */
	private File file;

	/** 最长等待时间 */
	private int awaitTime;
	
	/**	线程倒计时器	*/
	public static CountDownLatch threadCountDown;
	
	public SplitFile(String filename, int blockSize, int awaitTime) {
		this.filename = filename;
		file = new File(this.filename);
		this.blockSize = blockSize;
		blockNum = (int) Math.ceil(file.length() / (double) blockSize);
		this.awaitTime = awaitTime;
	}

	/** 分割文件为block */
	public List<Block> split() {
		List<Block> blocks = new ArrayList<>();
        // 给子线程一个倒计时
        threadCountDown = new CountDownLatch(blockNum);
		// 读取并分割文件
		RandomAccessFile rfile;
		try {
			rfile = new RandomAccessFile(file, "r"); // 只读
			// 开始分割（按线程，一个线程一个block）
			for (int blockId = 0; blockId < blockNum; blockId++) {
				Block block = new Block(filename, blockSize, blockNum, blockId);
				SplitRunnable runnable = new SplitRunnable(blockId * blockSize, rfile, block);
				runnable.start();
				blocks.add(block);
			}
			// 等待所有子线程
			threadCountDown.await(awaitTime, TimeUnit.SECONDS);
			rfile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return blocks;
	}
	
	public int getBlockNum() {
		return blockNum;
	}

}
