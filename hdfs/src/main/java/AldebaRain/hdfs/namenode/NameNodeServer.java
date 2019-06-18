package AldebaRain.hdfs.namenode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"AldebaRain.hdfs.namenode"})
public class NameNodeServer {

	public static void main(String[] args) {
		System.setProperty("spring.config.name", "namenode");
		SpringApplication.run(NameNodeServer.class, args);
	}

}
