# kafka consumer coordinator
+ 支持一个或多个主题及其分区
+ 每个分区需要一个消费线程，如果一个线程消费 2 个分区会导致消费不一致的情况
+ 协调器选举出 leader，leader 将所有主题及其分区信息写入存储 `topic-partitions` 路径中
+ follower 每个协调线程初始化，初始时间戳为 0，需要从存储 `coordinator` 里更新时间戳才会更新
+ leader 定时检查是否所有消费线程是否已经准备就绪，如果准备就绪，则调整当前最大周期时间

# check rule
leader 检查存储 `progress` 中的每个消费线程的分区和日志时间戳，如果分区总数一致且每个消费线程都已经消费到最大消费时间戳，
则更新当前周期最大时间戳为下一周期

# dependency
使用 `zookeeper` 的选举和存储功能

# zookeeper
+ 自动选举功能
+ 存储每个消费线程当前的主题分区编号和日志对应时间戳 `progress`
+ 当前周期的最大时间戳 `coordinator`
+ 记录消费的主题和分区信息

# 共享使用流程
+ kafka 消费线程需要设置协调器初始化，需要设置同步的 topic 列表，和每个 topic 的分区列表
+ 消费协调器主线程会将同步的同步的 topic 和分区信息 写入到共享存储，并设置初始检查周期
+ 消费协调器从线程会从共享存储读取数据

# 共享存储数据结构
+ `/rootPath/coordinator`  
timestamp

+ `/rootPath/topic-partitions`  
topic1:partition0,partition1,partition2,partition3;
topic2:partition0,partition1,partition2,partition3;
topic3:partition0,partition1,partition2,partition3;

+ `/rootPath/progress`  
topic1.partition0:timestamp;topic1.partition1:timestamp;topic1.partition2:timestamp;topic1.partition3:timestamp;
topic2.partition0:timestamp;topic2.partition1:timestamp;topic2.partition2:timestamp;topic2.partition3:timestamp;
topic3.partition0:timestamp;topic3.partition1:timestamp;topic3.partition2:timestamp;topic3.partition3:timestamp;

# 周期调整
数据重跑的时候，需要将周期调整为最小计算周期，如果计算到当前，则可以关闭协调器或调整周期为每天

# 实现原理
1. 各客户端 `KCCordinator` 初始化，包含的功能是 创建 ZK 路径，设置初始时间和当前周期的最大时间，并在个分区的线程进行选举，选举结束，根据选举结果，会产生 leader 和 follower
2. 如果是 leader，初始化的功能为：更新分区信息到 zk 的 `topic-partitions` 这个节点中，更新 leader 当前的 currentMaxTimestamp 的信息到 `coordinate` 节点中
3. 如果是 follower，不做任何初始化功能
4. 各消费线程开始消费数据，通过 `checkLimit(long timestamp)` 方法测试是否到了当前最大周期，如果返回为 false，则当前线程需要时间同步
5. leader 线程的时间同步：调用 `currentPeriodComplete(List<TopicPartition> topicPartitions)` 后，把 `currentMaxTimestamp` 的值写入到 `progress` 目录下，
循环检查 `progress` 目录下所有的分区是否都到了同步时间，如果都到了同步时间，则更新 `coordinator` 里的时间为 `currentMaxTimestamp + period`，否则一直等待
6. follower 线程的时间同步：调用 `currentPeriodComplete(List<TopicPartition> topicPartitions)` 后，把 `currentMaxTimestamp` 的值写入到 `progress` 目录下，
循环检查 `coordinator` 里的值，如果值为 `currentMaxTimestamp + period` 时，则设置 `currentMaxTimestamp` 的只同步过来的值，否则一直等待
























