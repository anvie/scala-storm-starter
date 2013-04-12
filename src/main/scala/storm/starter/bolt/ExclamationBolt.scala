package storm.starter.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.{BaseBatchBolt, BaseBasicBolt, BaseRichBolt}
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }
import java.util
import backtype.storm.coordination.BatchOutputCollector
import scala.collection.JavaConversions

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
    def execute(d: Tuple, col: BasicOutputCollector) {
        val text = d.getString(1)
        text.split("\\W+").foreach( splitedText => col.emit(new Values(d.getValue(0), splitedText) ))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(new Fields("id", "result"))
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

class AggregatorBolt extends BaseBatchBolt[AnyRef] {

    import JavaConversions._

    var col: BatchOutputCollector = _
    var id:AnyRef = _
    private val result = new util.HashSet[String]()

    def prepare(p1: util.Map[_, _], p2: TopologyContext, collector: BatchOutputCollector, _id:AnyRef) {
        col = collector
        id = _id
    }

    def execute(d: Tuple) {
        result.add(d.getString(1) + " - ")
    }

    def finishBatch() {
        col.emit(new Values(id, result.reduceLeftOption(_ + _).getOrElse("")))
    }

    def declareOutputFields(p1: OutputFieldsDeclarer) {
        p1.declare(new Fields("id", "output"))
    }
}

