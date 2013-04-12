package storm.starter.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.{BaseBasicBolt, BaseRichBolt}
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

class ExclamationBolt extends BaseRichBolt {
    var collector: OutputCollector = _

    override def prepare(config: JMap[_, _], context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
    }

    override def execute(tuple: Tuple) {
        this.collector.emit(tuple, new Values(Exclaimer.exclaim(tuple.getString(0))))
        this.collector.ack(tuple)
    }

    override def declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(new Fields("word"))
    }
}

class TextSplitterBolt extends BaseBasicBolt {
    def execute(p1: Tuple, p2: BasicOutputCollector) {

    }

    def declareOutputFields(p1: OutputFieldsDeclarer) {

    }
}

class BasicExclamationBolt extends BaseBasicBolt {
    def execute(d: Tuple, col: BasicOutputCollector) {
        col.emit(new Values(d.getValue(0), Exclaimer.exclaim(d.getString(1))))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(new Fields("id", "result"))
    }
}

