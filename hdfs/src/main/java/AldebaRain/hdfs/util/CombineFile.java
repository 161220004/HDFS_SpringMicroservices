package AldebaRain.hdfs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import AldebaRain.hdfs.Block;

public class CombineFile {

	private Logger logger = LoggerFactory.getLogger(CombineFile.class);
	
	/** 文件名 */
	private String filename;
	
	/** 各个block的数据 */
	List<Block> blocks;
	
	public CombineFile(String filename, List<Block> blocks) {
		this.filename = filename;
		this.blocks = blocks;
	}
	
	/** 写入文件 */
	public void write() {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 把blocks按照blockId排序
		Collections.sort(blocks, new Comparator<Block>() {
            public int compare(Block arg0, Block arg1) {
                return arg0.getBlockId().compareTo(arg1.getBlockId());
            }
        });
		try {
			OutputStream fos = new FileOutputStream(file);
			logger.info("Combine File '" + filename + "': ");
			for (Block block: blocks) {
				fos.write(block.getData(), 0, block.getLength());
				logger.info("--- Block " + block.getBlockId() + ": length = " + block.getLength());
			}
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
