package storm.starter.topology

import backtype.storm.{LocalDRPC, Config, LocalCluster, StormSubmitter}
import backtype.storm.testing.TestWordSpout
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import storm.starter.bolt.{AggregatorBolt, TextSplitterBolt, BasicExclamationBolt}
import backtype.storm.tuple.Fields

object Drpc {
    def main(args: Array[String]) {
        import storm.starter.bolt.ExclamationBolt

        val config = new Config()
        config.setDebug(true)

        if (args != null && args.length > 0) {
            config.setNumWorkers(3)

            val builder:TopologyBuilder = new TopologyBuilder()

            builder.setSpout("word", new TestWordSpout(), 10)
            builder.setBolt("exclaim", new ExclamationBolt(), 3)
                .shuffleGrouping("word")
            builder.setBolt("exclaim2", new ExclamationBolt(), 3)
                .shuffleGrouping("exclaim")

            StormSubmitter.submitTopology(args(0), config, builder.createTopology())
        } else {
            val builder = new LinearDRPCTopologyBuilder("exclamation")

            val drpc = new LocalDRPC()
            val cluster: LocalCluster = new LocalCluster()

            builder.addBolt(new TextSplitterBolt(), 2)
            builder.addBolt(new BasicExclamationBolt(), 3)
                .fieldsGrouping(new Fields("id"))
            builder.addBolt(new AggregatorBolt()).fieldsGrouping(new Fields("id"))
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
