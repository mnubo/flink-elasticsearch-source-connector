package com.mnubo.flink.streaming.connectors.elasticsearch1x

import org.apache.flink.core.io.InputSplit

case class Elasticsearch1xInputSplit(index: String,
                                     shard: Int,
                                     nodeHost: String,
                                     nodeId: String,
                                     nodePort: Int) extends InputSplit {
  val getSplitNumber = shard
}
