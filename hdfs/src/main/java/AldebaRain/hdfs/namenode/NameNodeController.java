package AldebaRain.hdfs.namenode;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.methodOn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.Resources;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.Main;
import AldebaRain.hdfs.util.SplitFile;

@Controller
public class NameNodeController {

	private Logger logger = LoggerFactory.getLogger(NameNodeController.class);

	@Autowired
	private DiscoveryClient discoveryClient;

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

	@Autowired
	private RestTemplate restTemplate;

	/** 当前所有DataNode服务实例 */
	private List<ServiceInstance> datanodeList;
	
	/** 当前所有文件在DataNode的的存储情况 */
	private Map<String, FileMap> fileMaps;
	
	@GetMapping("/namenode-debug")
    public @ResponseBody
    String debug() {
        logger.info("NameNode start: ");
        logger.info("NameNode: get services " + discoveryClient.getServices());
        this.refreshDataNodeList();
        String api = getApiByUri(String.valueOf(datanodeList.get(0).getUri()), "datanode-debug");
        String body = restTemplate.getForEntity(api, String.class).getBody();
        logger.info("Use" + body + ", Api = " + api);
        return "NameNode start: \n" + body;
    }

	/** 获取所有文件块在DataNode的分布情况 */
	@GetMapping("/map")
	public @ResponseBody 
	List<String> getDataNodeMap() {
		this.refreshFileMaps();
		List<String> fileList = new ArrayList<>();
		for (FileMap fileMap: fileMaps.values()) {
			fileList.add(fileMap.toString());
		}
		return fileList;
	}

	/** 上传文件 */
	@PostMapping("/upload")
	public @ResponseBody 
	Resources<Resource<String>> saveFile(@RequestParam String filename) {
		// 分割文件并保存至blocks
		SplitFile splitFile = new SplitFile(filename, Main.BlockSize);
		Map<Integer, byte[]> blocks = splitFile.split();
		int blockNum = splitFile.getBlockNum();
		// 刷新，获取当前活动的DataNode列表以及当前fileMaps
        this.refreshFileMaps();
        int datanodeNum = datanodeList.size();
        
        List<String> blockStrs = new ArrayList<>(); // 上传成功的显示信息
        // 发送所有块
        for (Integer blockId: blocks.keySet()) {
        	// 负载均衡：随机在DataNode列表中选择一个
        	int index = (int) (Math.random() * datanodeNum);
            // 封装发送的块数据
    		Block blockData = new Block(filename, blockNum, blockId, blocks.get(blockId));
    		// 调用DataNode的方法上传块
        	String uri = String.valueOf(datanodeList.get(index).getUri());
            String api = getApiByUri(uri, "blocks");
            String body = restTemplate.postForEntity(api, blockData, String.class).getBody();
            // 添加上传块的信息到fileMaps
            Block blockInfo = new Block(filename, blockNum, blockId);
            addBlockToMap(blockInfo, uri);
            
            blockStrs.add(new String(blocks.get(blockId)));
        }
    	List<Resource<String>> blockRes = blockStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(blockRes
				, linkTo(methodOn(NameNodeController.class).saveFile(filename)).withSelfRel());
	}
	
	/** 根据主机和端口获取服务接口地址 */
	private String getApiByUri(String uri, String api) {
		return new String(uri + "/" + api);
	}
	
	/** 添加一个文件块信息到fileMaps */
	private void addBlockToMap(Block blockInfo, String uri) {
		String filename = blockInfo.getFilename();
		// 若该文件信息存在一部分
		if (fileMaps.containsKey(filename)) {
			fileMaps.get(filename).addBlock(blockInfo, uri);
		}
		else { // 首次添加该文件的块
			fileMaps.put(filename, 
					new FileMap(filename, blockInfo.getBlockNum(), 
							new BlockMap(filename, blockInfo.getBlockId(), uri)));
		}
	}

	/** 检测并获取当前所有DataNode的URI */
	private void refreshDataNodeList() {
		datanodeList = discoveryClient.getInstances("datanode-service");
		logger.info("NameNode: get all datanodes ");
		for (ServiceInstance instance: datanodeList) {
			if (instance != null)
				logger.info("----- datanode [uri = " + instance.getUri() + "]");
		}
	}
	
	/** 根据当前所有DataNode发的信息刷新fileMaps */
	private void refreshFileMaps() {
		// 刷新，获取当前活动的DataNode列表
        this.refreshDataNodeList();
        fileMaps = new HashMap<>();
		// 遍历所有DataNode取得信息
		for (ServiceInstance datanode: datanodeList) {
			String uri = String.valueOf(datanode.getUri());
            String api = getApiByUri(uri, "report");
            List<Block> blockInfos = restTemplate.exchange(api, HttpMethod.GET, 
                    null, new ParameterizedTypeReference<List<Block>>() {}).getBody();
            for (Block blockInfo: blockInfos) {
            	addBlockToMap(blockInfo, uri);
            }
		}
	}
	
}
