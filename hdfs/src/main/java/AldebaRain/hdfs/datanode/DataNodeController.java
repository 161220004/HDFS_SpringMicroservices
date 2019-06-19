package AldebaRain.hdfs.datanode;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.methodOn;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.datanode.blocks.*;

@Controller
public class DataNodeController {

	private Logger logger = LoggerFactory.getLogger(DataNodeController.class);
	
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

    /** BlockReport，告知NameNode自己拥有的block */
    @GetMapping("/report")
    public @ResponseBody 
    List<Block> blockReport() {
    	 List<BlockInfo> blockInfos = blockInfoRepository.findAll();
    	 List<Block> blocks = new ArrayList<>();
    	 for (BlockInfo blockInfo: blockInfos)
    		 blocks.add(new Block(blockInfo.getFilename(), blockInfo.getBlockNum(), blockInfo.getBlockId()));
    	return blocks;
    }

    @GetMapping("/blocks")
    public @ResponseBody 
    Resources<Resource<String>> showBlock() {
    	List<BlockData> blockList = blockDataRepository.findAll();
    	List<String> blockStrs = new ArrayList<>();
    	for (BlockData data: blockList) {
            int len = data.getLength();
        	int showNum = (8 < len) ? 8 : len; // 显示的byte数
    		byte[] partData = new byte[showNum];
    		System.arraycopy(data.getData(), 0, partData, 0, showNum);
    		blockStrs.add(data.getFilename() + 
    				"(" + data.getBlockId() + "/" + data.getBlockNum() + "): " + 
    				new String(partData) + "..., length = " + data.getLength());
    	}
    	List<Resource<String>> blockRes = blockStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(blockRes
				, linkTo(methodOn(DataNodeController.class).showBlock()).withSelfRel()
				);
    }
    
    /** 上传文件块 */
    @PostMapping("/blocks")
    public @ResponseBody 
    String saveBlock(@RequestBody Block block) {
    	BlockData blockData = new BlockData(block.getFilename(), 
    			block.getBlockNum(), block.getBlockId(), block.getData(), block.getLength());
    	blockDataRepository.save(blockData);
    	BlockInfo blockInfo = new BlockInfo(blockData.getFilename(), blockData.getBlockNum(), blockData.getBlockId());
		blockInfoRepository.save(blockInfo);
		return "Save Block Success";
    }
    
    /** 下载文件块 */
    @GetMapping("/blocks/{identityUrl}")
    public @ResponseBody 
    Block getBlock(@PathVariable String identityUrl) {
		// 转换URL的转义字符
    	String identity = "";
		try {
			identity = URLDecoder.decode(identityUrl, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	logger.info("DownLoad Block: " + identity + "(" + identityUrl + ")");
    	List<BlockData> blockDatas = blockDataRepository.findAllByIdentity(identity);
    	int index = (int) (Math.random() * blockDatas.size());
    	BlockData blockData = blockDatas.get(index);
    	return new Block(identity, blockData.getData(), blockData.getLength());
    }

    /** 删除文件块 */
    @DeleteMapping("/blocks/{identityUrl}")
    public @ResponseBody 
    String deleteBlock(@PathVariable String identityUrl) {
		// 转换URL的转义字符
    	String identity = "";
		try {
			identity = URLDecoder.decode(identityUrl, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	logger.info("Delete Block: " + identity + "(" + identityUrl + ")");
    	List<BlockInfo> blockInfos = blockInfoRepository.findAllByIdentity(identity);
    	List<BlockData> blockDatas = blockDataRepository.findAllByIdentity(identity);
    	for (BlockInfo blockInfo: blockInfos)
    		blockInfoRepository.delete(blockInfo);
    	for (BlockData blockData: blockDatas)
    		blockDataRepository.delete(blockData);
    	return new String("Delete Block " + identityUrl + "(copyNum = " + blockInfos.size() + ") Success");
    }
}
