package sk.elasticsearch.aggregations

import org.apache.lucene.index.AtomicReaderContext
import org.elasticsearch.common.io.stream.{StreamInput, StreamOutput}
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues
import org.elasticsearch.search.aggregations.InternalAggregation.{CommonFields, ReduceContext, Type}
import org.elasticsearch.search.aggregations.metrics.{InternalNumericMetricsAggregation, NumericMetricsAggregator, NumericValuesSourceMetricsAggregatorParser}
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric
import org.elasticsearch.search.aggregations.support._
import org.elasticsearch.search.aggregations.{Aggregator, AggregatorFactory, InternalAggregation}

import scala.collection.JavaConversions._
import scala.collection.mutable

object InternalCountDistinctN {
  val TYPE: Type = new Type("countdistinctn")
}

sealed case class InternalCountDistinctN(var set: mutable.HashSet[Long], var nam: String) extends InternalNumericMetricsAggregation.SingleValue(nam) {

  def this() = this(null, null)

  override def `type`(): Type = InternalCountDistinctN.TYPE

  override def reduce(reduceContext: ReduceContext): InternalAggregation = {
    val set = reduceContext.aggregations().foldLeft(new mutable.HashSet[Long]()) { case (s, obj) =>
      s ++ obj.asInstanceOf[InternalCountDistinctN].set
    }
    InternalCountDistinctN(set, name)
  }

  override def doXContentBody(builder: XContentBuilder, params: Params): XContentBuilder = {
    builder.field(CommonFields.VALUE, set.size)
    builder
  }

  override def writeTo(out: StreamOutput): Unit = {
    out.writeString(nam)
    out.writeLongArray(set.toArray)
  }

  override def readFrom(in: StreamInput): Unit = {
    nam = in.readString()
    name = nam
    set = new mutable.HashSet[Long]() ++ in.readLongArray()
  }

  override def value(): Double = {
    set.size
  }
}

class CountDistinctNAggregator(nam: String,
                               estimatedBucketsCount: Long,
                               aggregationContext: AggregationContext,
                               parent: Aggregator,
                               valuesSource: Option[ValuesSource.Numeric])
  extends NumericMetricsAggregator.SingleValue(nam, estimatedBucketsCount, aggregationContext, parent) {

  private var values: SortedNumericDoubleValues = _

  private val ord2set = mutable.Map[Long, mutable.HashSet[Long]]()

  override def shouldCollect(): Boolean = valuesSource.isDefined

  override def buildEmptyAggregation(): InternalAggregation = {
    InternalCountDistinctN(new mutable.HashSet[Long](), name)
  }

  override def buildAggregation(owningBucketOrdinal: Long): InternalAggregation = {
    ord2set.get(owningBucketOrdinal) match {
      case Some(set) => InternalCountDistinctN(set, name)
      case _ => buildEmptyAggregation()
    }
  }

  override def collect(docId: Int, bucketOrdinal: Long): Unit = {
    values.setDocument(docId)
    val list = mutable.ListBuffer[Long]()
    for (i <- 0 until values.count()) {
      list += values.valueAt(i).toLong
    }
    val newSet = ord2set.get(bucketOrdinal) match {
      case Some(set) => set ++ list
      case _ => new mutable.HashSet[Long]() ++ list
    }

    ord2set += (bucketOrdinal -> newSet)
  }

  override def setNextReader(reader: AtomicReaderContext): Unit = {
    values = valuesSource.get.doubleValues()
  }

  override def metric(owningBucketOrd: Long): Double = {
    valuesSource.isDefined match {
      case true => ord2set(owningBucketOrd).size
      case false => Double.NaN
    }
  }
}

class CountDistinctNAggregatorFactory(nam: String, config: ValuesSourceConfig[ValuesSource.Numeric])
  extends ValuesSourceAggregatorFactory[ValuesSource.Numeric](nam, InternalCountDistinctN.TYPE.name(), config) {

  override def createUnmapped(aggregationContext: AggregationContext,
                              parent: Aggregator): Aggregator = {
    new CountDistinctNAggregator(name, 0, aggregationContext, parent, None)
  }

  override def create(valuesSource: Numeric,
                      expectedBucketsCount: Long,
                      aggregationContext: AggregationContext,
                      parent: Aggregator): Aggregator = {
    new CountDistinctNAggregator(name, expectedBucketsCount, aggregationContext, parent, Some(valuesSource))
  }
}

class CountDistinctNPluginParser extends NumericValuesSourceMetricsAggregatorParser[InternalCountDistinctN](InternalCountDistinctN.TYPE) {

  override def createFactory(aggregationName: String, config: ValuesSourceConfig[Numeric]): AggregatorFactory = {
    new CountDistinctNAggregatorFactory(aggregationName, config)
  }
}
