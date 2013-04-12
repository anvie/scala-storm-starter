package storm.starter.topology

import backtype.storm.{LocalDRPC, Config, LocalCluster, StormSubmitter}
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import storm.starter.bolt.{AggregatorBolt, TextSplitterBolt, BasicExclamationBolt}
import backtype.storm.tuple.Fields
import backtype.storm.generated.DistributedRPC
import backtype.storm.utils.DRPCClient

object DrpcClient {
    def main(args: Array[String]) {

        val drpc = new DRPCClient("localhost", 3772)

        val rv1 = drpc.execute("exclamation", "hello robin is here")
        println("rv1: " + rv1)

        val rv2 = drpc.execute("words", "hello robin is here, robin sy.")
        println("rv2: " + rv2)

    }
}
