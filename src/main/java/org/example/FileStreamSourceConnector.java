package org.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * tutorial from <a href="https://docs.confluent.io/platform/current/connect/devguide.html#connector-example">...</a>
 */
public class FileStreamSourceConnector extends SourceConnector {

  // 配置项
  public static final String TOPIC_CONFIG = "topic";
  public static final String FILE_CONFIG = "file";
  public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

  // 默认批处理大小，最大一批2000行
  public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

  // 配置ConfigDef，可以通过ConfigDef自动从指定配置文件中取出配置映射
  // 在kafka-connect-jdbc中，这部分config是在单独的配置文件
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FILE_CONFIG, Type.STRING, null, ConfigDef.Importance.HIGH,
          "Source filename. If not specified, the standard input will be used")
      .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH,
          "The topic to publish data to")
      .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
          "The maximum number of records the Source task can read from file one time");

  // 将配置文件中的value解析出来保存在Connector本地
  private String filename;
  private String topic;
  private int batchSize;

  /**
   * props是由kafka-connect中解析配置文件的组件解析完成后传入的，
   * start作为生命周期的第一个函数，其职责是配置connector需要的参数
   *
   * @param props configuration settings
   */
  @Override
  public void start(Map<String, String> props) {
    // 取出配置文件
    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    filename = parsedConfig.getString(FILE_CONFIG);
    List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
    // 验证，因为FileStreamConnector做的是单文件，所以输入文件个数必须为1
    if (topics.size() != 1) {
      throw new ConfigException("'topic' in FileStreamSourceConnector configuration requires definition of a single topic");
    }
    // 取出0号topic
    topic = topics.get(0);
    batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
  }

  /**
   * 关联SourceConnector对应的Task
   */
  @Override
  public Class<? extends Task> taskClass() {
    return FileStreamSourceTask.class;
  }

  /**
   * Task需要的属性，在这里装配
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>();
    if (filename != null) {
      config.put(FILE_CONFIG, filename);
    }
    config.put(TOPIC_CONFIG, topic);
    config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
    configs.add(config);
    return configs;
  }

  /**
   * 生命周期最后一个函数，用于收尾工作，例如关闭JDBCConnection等，由于本例读取本地文件所以不需要释放资源
   */
  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    // 返回ConfigDef对象，交给上层做配置文件解析
    return CONFIG_DEF;
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }
}
