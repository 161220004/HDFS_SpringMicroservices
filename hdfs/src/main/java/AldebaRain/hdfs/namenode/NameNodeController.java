package AldebaRain.hdfs.namenode;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.methodOn;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import AldebaRain.hdfs.Block;
import AldebaRain.hdfs.namenode.maps.BlockMap;
import AldebaRain.hdfs.namenode.maps.FileMap;
import AldebaRain.hdfs.util.CombineFile;
import AldebaRain.hdfs.util.SplitFile;

@Controller
public class NameNodeController {

	private Logger logger = LoggerFactory.getLogger(NameNodeController.class);

	@Autowired
	private DiscoveryClient discoveryClient;

	@Autowired
	private HttpServletRequest httpServletRequest;
	
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
	@PostMapping("/files")
	public @ResponseBody 
	Resources<Resource<String>> saveFile(@RequestParam String filename) {
		// 分割文件并保存至blocks
		SplitFile splitFile = new SplitFile(filename);
		List<Block> blocks = splitFile.split();
		int blockNum = splitFile.getBlockNum();
		// 刷新，获取当前活动的DataNode列表以及当前fileMaps
        this.refreshFileMaps();
        
        List<String> blockStrs = new ArrayList<>(); // 上传成功的显示信息
        // 发送所有块
        for (Block block: blocks) {
        	// 负载均衡：随机在DataNode列表中选择一个
        	int index = (int) (Math.random() * datanodeList.size());
            // 封装发送的块数据
        	Integer blockId = block.getBlockId();
    		Block blockData = new Block(filename, blockNum, blockId, block.getData(), block.getLength());
    		// 调用DataNode的方法上传块
        	String uri = String.valueOf(datanodeList.get(index).getUri());
            String api = getApiByUri(uri, "blocks");
            restTemplate.postForEntity(api, blockData, String.class).getBody();
            // 添加上传块的信息到fileMaps
            Block blockInfo = new Block(filename, blockNum, blockId);
            addBlockToMap(blockInfo, uri);
            // 便于显示结果
            int len = block.getLength();
        	int showNum = (8 < len) ? 8 : len; // 显示的byte数
    		byte[] partData = new byte[showNum];
    		System.arraycopy(block.getData(), 0, partData, 0, showNum);
            blockStrs.add(new String(partData));
        }
    	List<Resource<String>> blockRes = blockStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(blockRes
				, linkTo(methodOn(NameNodeController.class).saveFile(filename)).withSelfRel());
	}
	
	/** 下载文件 
	 * @param filenameUrl: '/'和'\'在URL里面有点问题，替换成了'?' */
	@GetMapping("/files/**")
	public @ResponseBody 
	Resources<Resource<String>> getFile() {
		String path = httpServletRequest.getRequestURI();
		String filename = getFilenameFromUrl(path.substring(new String("/files/").length()));
		//String[] pathStrs = filename.split("[/]");
    	logger.info("DownLoad File: " + filename);
    	// 下载成功的显示信息
        List<String> uriStrs = new ArrayList<>(); 
		// 刷新，获取当前活动的DataNode列表以及当前fileMaps
        this.refreshFileMaps();
        //if (fileMaps.containsKey(filename))
        FileMap fileMap = fileMaps.get(filename);
        Map<Integer, BlockMap> blockMaps = fileMap.getBlockMaps();
        // 获取byte数组的List
        List<Block> blockList = new ArrayList<>();
        for (BlockMap blockMap: blockMaps.values()) {
        	Integer blockId = blockMap.getBlockId();
        	String identity = Block.toIdentity(filename, blockId);
        	String identityUrl = Block.toIdentityUrl(filename, blockId);
        	// 随机选择一个含有该块的DataNode
        	List<String> uris = blockMap.getUris();
        	int index = (int) (Math.random() * uris.size());
    		// 调用DataNode的方法下载块
        	String uri = uris.get(index);
            String api = getApiByUri(uri, new String("blocks/" + identityUrl));
        	logger.info("DownLoad Block: " + identity + ", url = " + api);
            Block block = restTemplate.getForEntity(api, Block.class).getBody();
            blockList.add(block);
        	// 便于显示结果
            uriStrs.add(new String(blockId + ":" + uri + ", "));
        }
        // 组合块并保存
        CombineFile combineFile = new CombineFile(filename, blockList);
        combineFile.write();
        
    	List<Resource<String>> uriRes = uriStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(uriRes
				, linkTo(methodOn(NameNodeController.class).saveFile(filename)).withSelfRel());
	}

	/** 删除 
	 * @param filenameUrl: '/'和'\'在URL里面有点问题，替换成了'?' */
	@DeleteMapping("/files/**")
	public @ResponseBody 
	Resources<Resource<String>> deleteFile() {
		String path = httpServletRequest.getRequestURI();
		String filename = getFilenameFromUrl(path.substring(new String("/files/").length()));
		//String[] pathStrs = filename.split("[/]");
    	logger.info("Delete File: " + filename);
    	// 下载成功的显示信息
        List<String> uriStrs = new ArrayList<>(); 
		// 刷新，获取当前活动的DataNode列表以及当前fileMaps
        this.refreshFileMaps();
        //if (fileMaps.containsKey(filename))
        FileMap fileMap = fileMaps.get(filename);
        Map<Integer, BlockMap> blockMaps = fileMap.getBlockMaps();
        for (BlockMap blockMap: blockMaps.values()) {
        	Integer blockId = blockMap.getBlockId();
        	String identity = Block.toIdentity(filename, blockId);
        	String identityUrl = Block.toIdentityUrl(filename, blockId);
        	// 遍历全部含有该块的DataNode
        	List<String> uris = blockMap.getUris();
        	for (String uri: uris) {
        		// 调用DataNode的方法删除块
                String api = getApiByUri(uri, new String("blocks/" + identityUrl));
            	logger.info("Delete Block: " + identity + ", url = " + api);
                restTemplate.delete(api);
            	// 便于显示结果
                uriStrs.add(new String(blockId + ":" + uri + ", "));
        	}
        }
		// 刷新当前fileMaps
        fileMaps.remove(filename);
        
    	List<Resource<String>> uriRes = uriStrs.stream()
                .map(str -> new Resource<>(str)).collect(Collectors.toList());
		return new Resources<>(uriRes
				, linkTo(methodOn(NameNodeController.class).saveFile(filename)).withSelfRel());
	}
	
	/** 根据主机和端口获取服务接口地址 */
	private String getApiByUri(String uri, String api) {
		return new String(uri + "/" + api);
	}
	
	/** 将文件名URL转成filename */
	private String getFilenameFromUrl(String filenameUrl) {
		// 转换URL的转义字符
    	String filename = "";
		try {
			filename = URLDecoder.decode(filenameUrl, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return filename;
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
