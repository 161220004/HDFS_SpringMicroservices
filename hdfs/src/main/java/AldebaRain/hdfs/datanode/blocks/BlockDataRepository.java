package AldebaRain.hdfs.datanode.blocks;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BlockDataRepository extends JpaRepository<BlockData, Integer> {

	List<BlockData> findAllByIdentity(String identity);

}
