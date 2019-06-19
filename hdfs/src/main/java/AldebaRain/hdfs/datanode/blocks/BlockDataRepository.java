package AldebaRain.hdfs.datanode.blocks;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface BlockDataRepository extends JpaRepository<BlockData, Integer> {

	List<BlockData> findAllByIdentity(String identity);

}
