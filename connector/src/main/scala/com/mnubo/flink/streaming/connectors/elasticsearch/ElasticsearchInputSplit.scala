package com.mnubo.flink.streaming.connectors.elasticsearch

import org.apache.flink.core.io.InputSplit

case class ElasticsearchInputSplit(index: String,
                                   shard: Int,
                                   nodeHost: String,
                                   nodeId: String,
                                   nodePort: Int) extends InputSplit {
  val getSplitNumber = shard
}
