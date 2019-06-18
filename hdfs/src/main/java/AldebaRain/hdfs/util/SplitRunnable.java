package AldebaRain.hdfs.util;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SplitRunnable implements Runnable {

	/**	线程	*/
	protected Thread thread; 
	
	/** 该文件块命名 */
	private String blockName;
	
	/** 源文件 */
	private RandomAccessFile rfile;
	
	/** 分割的起始位置 */
	private int startPos;

	/** 最终获取的分割文件 */
	private byte[] block;
	
	public SplitRunnable(String blockName, int startPos, RandomAccessFile rfile, byte[] block) {
		this.startPos = startPos;
		this.blockName = blockName;
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
//		OutputStream fos;
		try {
			// 移动到分割点
			rfile.seek(startPos);
			// 从分割起点开始读取
			int bNum = rfile.read(block);
			System.out.println(blockName + ": Split " + bNum + "B (" + Math.round((double) bNum / (1024.0 * 1024.0)) + "M)");
			
			// 写入为块文件
//			fos = new FileOutputStream(blockName);
//			fos.write(block, 0, bNum);
//			fos.flush();
//			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
