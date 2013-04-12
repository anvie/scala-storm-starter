package storm.starter.topology

import backtype.storm.{LocalDRPC, Config, LocalCluster, StormSubmitter}
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import storm.starter.bolt.{AggregatorBolt, TextSplitterBolt, BasicExclamationBolt}
import backtype.storm.tuple.Fields
import backtype.storm.utils.DRPCClient

object Drpc {
    def main(args: Array[String]) {

        val builder = new LinearDRPCTopologyBuilder("exclamation")


        val cluster: LocalCluster = new LocalCluster()

        builder.addBolt(new TextSplitterBolt(), 2)
        builder.addBolt(new BasicExclamationBolt(), 3)
            .fieldsGrouping(new Fields("id"))
        builder.addBolt(new AggregatorBolt(), 2).fieldsGrouping(new Fields("id"))

        val config = new Config()
        config.setDebug(true)

        if (args != null && args.length > 0) {
            config.setNumWorkers(3)
            StormSubmitter.submitTopology(args(0), config, builder.createRemoteTopology())

            val drpc = new DRPCClient("127.0.0.1", 3772)
            val rv = drpc.execute("exclamation", "hello robin is here")
            println("rv: " + rv)

        } else {

            val drpc = new LocalDRPC()
//                .shuffleGrouping("word")

            cluster.submitTopology("ExclamationTopology", config, builder.createLocalTopology(drpc))

            val rv = drpc.execute("exclamation", "hello robin is here")
            println("rv: " + rv)

//            Utils.sleep(5000)
//            cluster.killTopology("ExclamationTopology")
            cluster.shutdown()
            drpc.shutdown()
        }
    }
}
