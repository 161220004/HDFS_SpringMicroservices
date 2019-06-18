package AldebaRain.hdfs.datanode;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.methodOn;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.Resources;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.datanode.blocks.*;

@Controller
public class DataNodeController {

	private Logger logger = LoggerFactory.getLogger(DataNodeServer.class);
	
	@Autowired
    private Registration registration; // 服务注册

	@Autowired
	private BlockInfoRepository blockInfoRepository;

	@Autowired
	private BlockDataRepository blockDataRepository;
	
    @GetMapping("/datanode-debug")
    public @ResponseBody
    String debug() {
		String instanceId = registration.getServiceId();
		String uri = String.valueOf(registration.getUri());
		logger.info("DataNode (" + instanceId + ") start at port " + registration.getPort() + ": uri = " + uri);
        return "DataNode (" + instanceId + ") start at port " + registration.getPort() + "\n    uri = " + uri;
    }

    @GetMapping("/report")
    public @ResponseBody 
    List<BlockInfo> blockReport() {
    	return blockInfoRepository.findAll();
    }

    @GetMapping("/blocks")
    public @ResponseBody 
    Resources<Resource<String>> showBlock() {
    	List<BlockData> blockList = blockDataRepository.findAll();
    	List<String> blockStrs = new ArrayList<>();
    	int showNum = 8; // 显示的byte数
    	for (BlockData data: blockList) {
    		byte[] partData = new byte[showNum];
    		System.arraycopy(data.getData(), 0, partData, 0, showNum);
    		blockStrs.add(data.getFilename() + 
    				"(" + data.getBlockId() + "/" + data.getBlockNum() + "), show part: " + 
    				new String(data.getData()) + "...");
    	}
    	List<Resource<String>> blockRes = blockStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(blockRes
				, linkTo(methodOn(DataNodeController.class).showBlock()).withSelfRel()
				);
    }
    
    @PostMapping("/blocks")
    public @ResponseBody 
    String saveBlock(@RequestBody Block block) {
    	BlockData blockData = new BlockData(block.getFilename(), 
    			block.getBlockNum(), block.getBlockId(), block.getData());
    	blockDataRepository.save(blockData);
    	BlockInfo blockInfo = new BlockInfo(blockData.getFilename(), blockData.getBlockNum(), blockData.getBlockId());
		blockInfoRepository.save(blockInfo);
		return "Save Block Success";
    }
    
    
}
