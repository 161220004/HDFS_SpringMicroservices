package AldebaRain.hdfs.datanode.blocks;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BlockInfoRepository extends JpaRepository<BlockInfo, Integer> {

	BlockInfo findByIdentity(String identity);
	
}
