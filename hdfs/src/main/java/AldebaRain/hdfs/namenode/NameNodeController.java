package AldebaRain.hdfs.namenode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import AldebaRain.hdfs.Main;
import AldebaRain.hdfs.namenode.util.SplitFile;

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
	
	@PostMapping("/saveFile")
	public @ResponseBody 
    String saveFile(@RequestBody String fileName) {
		SplitFile splitFile = new SplitFile(fileName, Main.BlockSize);
		Map<Integer, byte[]> blocks = splitFile.split();
		
		return "Save File Success";
	}
	
	/** 根据主机和端口获取服务接口地址 */
	private String getApiByUri(String uri, String api) {
		return new String(uri + "/" + api);
	}
	
	
	/** 检测并获取当前所有datanode的id */
	private void refreshDataNodeList() {
		datanodeList = discoveryClient.getInstances("datanode-service");
		logger.info("NameNode: get datanodes ");
		for (ServiceInstance instance: datanodeList) {
			if (instance != null)
				logger.info("----- datanode [uri = " + instance.getUri() + "]");
		}
	}
	
	/** 向 */
	
}
