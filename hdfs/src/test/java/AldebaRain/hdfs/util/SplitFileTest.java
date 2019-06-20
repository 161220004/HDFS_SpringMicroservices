package AldebaRain.hdfs.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.util.SplitFile;

public class SplitFileTest {
	
	@Test
	public void writeFile() throws IOException, InterruptedException {
		long originFileSize = 50;// 50B
		// 生成一个大文件
		String fileName = "SplitFileTest_File";
		File file = new File(fileName);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		String str = new String("A");
		int len = str.getBytes().length;
		for(int i = 0; i < originFileSize / len; i++) {
            bufferedWriter.write(str);
        }
        bufferedWriter.close();

        System.out.println("Create File Success");
        
		SplitFile splitFile = new SplitFile(fileName, 64);
		List<Block> blocks = splitFile.split();
		for (Block block: blocks) {
			byte[] tmp = Arrays.copyOf(block.getData(), 8);
			System.out.println(block.getFilename() + "(" + block.getBlockId() + ") : " + 
					new String(tmp) + " (" + block.getLength() + " B)");
		}
		
	}
}
