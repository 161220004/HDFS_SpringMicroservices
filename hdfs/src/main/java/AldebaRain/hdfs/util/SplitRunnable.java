package AldebaRain.hdfs.util;

import java.io.IOException;
import java.io.RandomAccessFile;

import AldebaRain.hdfs.Block;

public class SplitRunnable implements Runnable {

	/**	线程	*/
	protected Thread thread; 
	
	/** 源文件 */
	private RandomAccessFile rfile;
	
	/** 分割的起始位置 */
	private int startPos;

	/** 最终获取的分割文件 */
	private Block block;
	
	public SplitRunnable(int startPos, RandomAccessFile rfile, Block block) {
		this.startPos = startPos;
		this.rfile = rfile;
		this.block = block;
	}

	/**	开启线程	*/
	public void start() {
    	thread = new Thread(this);
    	thread.start();
    	try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		try {
			// 移动到分割点
			rfile.seek(startPos);
			// 从分割起点开始读取
			int bNum = rfile.read(block.getData());
			block.setLength(bNum);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	SplitFile.threadCountDown.countDown();
	}

}
