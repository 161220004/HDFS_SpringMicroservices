package AldebaRain.hdfs.datanode;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import AldebaRain.hdfs.datanode.blocks.*;

@Controller
public class DataNodeController {

	private Logger logger = LoggerFactory.getLogger(DataNodeServer.class);
	
    @Value("${server.port}")
    String port;

	@Autowired
    private Registration registration; // 服务注册

	@Autowired
	private BlockInfoRepository blockInfoRepository;
	
    @GetMapping("/datanode-debug")
    public @ResponseBody
    String debug() {
		String instanceId = registration.getServiceId();
		logger.info("DataNode start at port " + port + ": id = " + instanceId);
        return "DataNode start at port " + port + ": id = " + instanceId;
    }

    @GetMapping("/report")
    public @ResponseBody 
    List<BlockInfo> blockReport() {
		return blockInfoRepository.findAll();
    }
    
    @PostMapping("/saveblock")
    public @ResponseBody 
    String saveBlock(String filename, String blockId, byte[] block) {
    	String blockName = filename + "." + blockId;
    	OutputStream fos;
    	String outInfo;
    	try {
			fos = new FileOutputStream(blockName);
			fos.write(block, 0, block.length);
			fos.flush();
			fos.close();
			blockInfoRepository.save(new BlockInfo(filename, blockId));
			outInfo = "Save Block " + blockId + " of " + filename + " Success";
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			outInfo = "Save Block " + blockId + " of " + filename + " Failed";
		} catch (IOException e) {
			e.printStackTrace();
			outInfo = "Save Block " + blockId + " of " + filename + " Failed";
		}
    	logger.info("DataNode saveBlock: " + outInfo);
		return outInfo;
    }
    
    
}
