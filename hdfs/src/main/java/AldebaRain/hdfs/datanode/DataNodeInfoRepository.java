package AldebaRain.hdfs.datanode;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataNodeInfoRepository extends JpaRepository<DataNodeInfo, Integer> {

}
