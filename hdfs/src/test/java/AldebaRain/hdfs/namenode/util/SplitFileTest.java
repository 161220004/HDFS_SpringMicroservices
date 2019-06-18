package AldebaRain.hdfs.namenode.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import AldebaRain.hdfs.util.SplitFile;

public class SplitFileTest {
	
	@Test
	public void writeFile() throws IOException, InterruptedException {
		long originFileSize = 1024 * 1024 * 10;// 10M
		// 生成一个大文件
		String fileName = "test_file";
		File file = new File(fileName);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		String str = new String("ABC");
		int len = str.getBytes().length;
		for(int i = 0; i < originFileSize / len; i++) {
            bufferedWriter.write(str);
        }
        bufferedWriter.close();

        System.out.println("Create File Success");
        
		int blockFileSize = 1024 * 1024 * 4;// 4M
		SplitFile splitFile = new SplitFile(fileName, blockFileSize);
		Map<Integer, byte[]> blocks = splitFile.split();
		for (Map.Entry<Integer, byte[]> block: blocks.entrySet()) {
			byte[] tmp = Arrays.copyOf(block.getValue(), 9);
			System.out.println(splitFile.getBlockName(block.getKey()) + ": Part is " + Arrays.toString(tmp) + 
					" -> " + new String(tmp) + 
					" (" + block.getValue().length / (1024 * 1024) + "M)");
		}
		
	}
}
