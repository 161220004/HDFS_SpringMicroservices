package AldebaRain.hdfs.datanode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"AldebaRain.hdfs.datanode"})
public class DataNodeServer {

	public static void main(String[] args) {
		System.setProperty("spring.config.name", "datanode");
		SpringApplication.run(DataNodeServer.class, args);
	}

}
