package org.example;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileStreamSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);

  public static final String FILENAME_FIELD = "filename";
  public static final String POSITION_FIELD = "position";
  private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

  // 需要读取的文件名
  private String filename;
  // 输入流
  private InputStream stream;
  // reader
  private BufferedReader reader = null;
  // buffer数组
  private char[] buffer;
  // 由Task维护的偏移量
  private int offset = 0;
  // 需要保存的topic
  private String topic;
  // 批处理大小
  private int batchSize = FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE;
  // 输入流的偏移量位置
  private Long streamOffset;

  public FileStreamSourceTask() {
    this(1024);
  }

  // buffer大小
  FileStreamSourceTask(int initialBufferSize) {
    buffer = new char[initialBufferSize];
  }


  /**
   * 配置
   *
   * @param props initial configuration
   */
  @Override
  public void start(Map<String, String> props) {
    filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
    // 验证filename，若未设置，则从控制台中获取
    if (filename == null || filename.isEmpty()) {
      stream = System.in;
      streamOffset = null;
      reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }
    // Missing topic or parsing error is not possible because we've parsed the config in the Connector
    topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
    batchSize = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));
  }

  /**
   * poll做实际的拉取数据，整理成SourceRecord
   */
  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (stream == null) {
      try {
        stream = Files.newInputStream(Paths.get(filename));
        // offset()方法接收Map<String,String>分区，通过分区确认偏移量，并返回Map<String, Object> offset
        // 本例中，通过 Map<"file", "/path/filename">来做分区，实际连接数据库需要Map<table, partition>
        Map<String, Object> offsetMap = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
        if (offsetMap != null) {
          Object lastRecordedOffset = offsetMap.get(POSITION_FIELD);
          if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) {
            throw new ConnectException("Offset position is the incorrect type");
          }
          if (lastRecordedOffset != null) {
            log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
            long skipLeft = (Long) lastRecordedOffset;
            while (skipLeft > 0) {
              try {
                long skipped = stream.skip(skipLeft);
                skipLeft -= skipped;
              } catch (IOException e) {
                log.error("Error while trying to seek to previous offset in file {}: ", filename, e);
                throw new ConnectException(e);
              }
            }
            log.debug("Skipped to offset {}", lastRecordedOffset);
          }
          streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
        } else { // 偏移量为空
          streamOffset = 0L;
        }
        reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        log.debug("Opened {} for reading", logFilename());
      } catch (NoSuchFileException e) {
        log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
        synchronized (this) {
          this.wait(1000);
        }
      } catch (IOException e) {
        log.error("Error while trying to open file {}: ", filename, e);
        throw new ConnectException(e);
      }
    }

    try {
      final BufferedReader readerCopy;
      synchronized (this) {
        readerCopy = reader;
      }
      if (readerCopy == null) {
        return null;
      }

      List<SourceRecord> records = null;
      int nread = 0;
      while (readerCopy.ready()) {
        nread = readerCopy.read(buffer, offset, buffer.length - offset);
        log.trace("Read {} bytes from {}", nread, logFilename());

        if (nread > 0) {
          offset += nread;
          String line;
          boolean foundOneLine = false;
          do {
            line = extractLine();
            if (line != null) {
              foundOneLine = true;
              log.trace("Read a line from {}", logFilename());
              if (records == null) {
                records = new ArrayList<>();
              }
              records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, null,
                  null, null, VALUE_SCHEMA, line, System.currentTimeMillis()));
              if (records.size() >= batchSize) {
                return records;
              }
            }
          } while (line != null);

          // 若没有读取到任何一行字符串 并且 偏移量以及移动到buffer的末尾，也就是说buffer长度<一行字符串
          if (!foundOneLine && offset == buffer.length) {
            char[] newBuf = new char[buffer.length * 2];
            System.arraycopy(buffer, 0, newBuf, 0, buffer.length);
            log.info("Increased buffer from {} to {}", buffer.length, newBuf.length);
            buffer = newBuf;
          }
        }
      }

      // 若没有读到任何字符，则等待1秒
      if (nread <= 0) {
        synchronized (this) {
          this.wait(1000);
        }
      }
      return records;
    } catch (IOException e) {

    }
    return null;
  }

  private String extractLine() {
    int until = -1, newStart = -1;
    for (int i = 0; i < offset; i++) {
      if (buffer[i] == '\n') {
        until = i;
        newStart = i + 1;
        break;
      } else if (buffer[i] == '\r') {
        if (i + 1 >= offset) {
          return null;
        }
        until = i;
        newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
        break;
      }
    }
    // 遍历出一行字符串
    if (until != -1) {
      String result = new String(buffer, 0, until);
      System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
      // 偏移量也做更新，因为读取出了一部分，剩下的未读取
      offset = offset - newStart;
      // 流偏移量也更新，向前进newStart个单位
      if (streamOffset != null) {
        streamOffset += newStart;
      }
      return result;
    } else {
      return null;
    }
  }

  private Map<String, String> offsetKey(String filename) {
    return Collections.singletonMap(FILENAME_FIELD, filename);
  }

  private Map<String, Long> offsetValue(Long pos) {
    return Collections.singletonMap(POSITION_FIELD, pos);
  }

  @Override
  public void stop() {
    log.trace("Stopping");
    synchronized (this) {
      try {
        if (stream != null && stream != System.in) {
          stream.close();
          log.trace("Closed input stream");
        }
      } catch (IOException e) {
        log.error("Failed to close FileStreamSourceTask stream: ", e);
      }
      this.notify();
    }
  }

  @Override
  public String version() {
    return new FileStreamSourceConnector().version();
  }

  private String logFilename() {
    return filename == null ? "stdin" : filename;
  }

  /* visible for testing */
  int bufferSize() {
    return buffer.length;
  }
}
