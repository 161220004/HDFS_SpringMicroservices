# HDFS_SpringMicroservices

- 微服务架构的分布式文件系统设计与实现

- Demo请观看demo目录下的mp4文件



### 运行方式

先执行 `mvn install` ，则会在“...\HDFS_SpringMicroservices\hdfs\target”目录下生成文件“**hdfs-0.0.1-SNAPSHOT.jar**”

在“...\HDFS_SpringMicroservices\hdfs”目录下使用命令行执行：

- Registration，可采取：

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar reg
    ```

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar registration
    ```

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar reg 8081
    ```

  - ……

- NameNode，可采取：

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar name
    ```

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar namenode
    ```

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar name 8090
    ```

  - ……

- DataNode，可采取（不同实例的端口必须不同）：

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar data 8091
    ```

  - ```
    java -jar target/hdfs-0.0.1-SNAPSHOT.jar datanode 8092
    ```

  - ###### ……



### 基本接口

客户端电脑磁盘为“**...\HDFS_SpringMicroservices\hdfs\client**”，一切上传的文件从此根目录获取；一切下载的文件下载到此根目录

（以 NameNode 端口为默认的 8090 为例）

- 文件上传：

  POST：http://localhost:8090/files（传入参数为文件名，例如："client/more/testfile"）

- 文件下载（以文件地址为"more/testfile"为例）：

  GET：http://localhost:8090/files/client/more/testfile

- 文件删除（以文件地址为"more/testfile"为例）：

  DELETE：http://localhost:8090/files/client/more/testfile

- 其他（便于测试的接口）

  - 查看 NameNode 当前所有文件的所有 Block 在所有 DataNode 中的存储情况（BlockMap）：

    GET：http://localhost:8090/map

  - 查看某一 DataNode 存储的所有 Block（以 DataNode 端口为 8091 为例）：

    GET：http://localhost:8091/blocks



### 测试方式

开启一个 Registration 实例，一个 NameNode 实例和多个端口不同的 DataNode 实例，使用 **PostMan** 测试上述接口



### 配置文件

可修改参数的配置文件为“...\HDFS_SpringMicroservices\hdfs\src\main\resources\configs.properties”

可修改的参数有：

- BlockSize：文件 Block 大小，默认为 64B（较小，以便测试）
- CopyNum：文件存储的副本数，默认为 3
- ShowNum：在使用 PostMan 时返回的 Block 中显示的字节数（方便查看，不影响系统运行机制），默认为 32B
- AwaitTime：分割文件成 Block 时的最长等待时间，默认为 60s（若文件过大，此时间可能需要延长）







