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

object InternalCountDistinct {
  val TYPE: Type = new Type("countdistinct")
}

sealed case class InternalCountDistinct(var set: mutable.HashSet[Long], var nam: String) extends InternalNumericMetricsAggregation.SingleValue(nam) {

  def this() = this(null, null)

  override def `type`(): Type = InternalCountDistinct.TYPE

  override def reduce(reduceContext: ReduceContext): InternalAggregation = {
    val set = reduceContext.aggregations().foldLeft(new mutable.HashSet[Long]()) { case (set, obj) =>
      set ++ obj.asInstanceOf[InternalCountDistinct].set
    }
    InternalCountDistinct(set, name)
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
    set = new mutable.HashSet[Long] () ++ (in.readLongArray())
  }

  override def value(): Double = set.size
}

class CountDistinctAggregator(nam: String,
                    estimatedBucketsCount: Long,
                    aggregationContext: AggregationContext,
                    parent: Aggregator,
                    valuesSource: Option[ValuesSource.Numeric])
  extends NumericMetricsAggregator.SingleValue(nam, estimatedBucketsCount, aggregationContext, parent) {

  private var values: SortedNumericDoubleValues = null

  private val ord2set = mutable.Map[Long, mutable.HashSet[Long]] ()

  override def shouldCollect(): Boolean = valuesSource.isDefined

  override def buildEmptyAggregation(): InternalAggregation = {
    InternalCountDistinct(new mutable.HashSet[Long](), name)
  }

  override def buildAggregation(owningBucketOrdinal: Long): InternalAggregation = {
    ord2set.get(owningBucketOrdinal) match {
      case Some(set) => InternalCountDistinct(set, name)
      case _ => buildEmptyAggregation()
    }
  }

  override def collect(docId: Int, bucketOrdinal: Long): Unit = {
    values.setDocument(docId)
    val list = mutable.ListBuffer[Long]()
    for (i <- 0 to values.count()) {
      list += values.valueAt(i).toLong
    }
    val newSet = ord2set.get(bucketOrdinal) match {
      case Some(set) => set ++ list
      case _ => new mutable.HashSet[Long]() ++ (list)
    }

    ord2set += (bucketOrdinal -> newSet)
  }

  override def setNextReader(reader: AtomicReaderContext): Unit = {
    values = valuesSource.get.doubleValues()
  }

  override def metric(owningBucketOrd: Long): Double = {
    valuesSource.isDefined match {
      case true => ord2set.get(owningBucketOrd).get.size
      case false => Double.NaN
    }
  }
}

class CountDistinctAggregatorFactory(nam: String, config: ValuesSourceConfig[ValuesSource.Numeric])
  extends ValuesSourceAggregatorFactory[ValuesSource.Numeric](nam, InternalCountDistinct.TYPE.name(), config) {

  override def createUnmapped(aggregationContext: AggregationContext,
                              parent: Aggregator): Aggregator = {
    new CountDistinctAggregator(name, 0, aggregationContext, parent, None)
  }

  override def create(valuesSource: Numeric,
                      expectedBucketsCount: Long,
                      aggregationContext: AggregationContext,
                      parent: Aggregator): Aggregator = {
    new CountDistinctAggregator(name, expectedBucketsCount, aggregationContext, parent, Some(valuesSource))
  }
}

class CountDistinctPluginParser extends NumericValuesSourceMetricsAggregatorParser[InternalCountDistinct](InternalCountDistinct.TYPE) {

  override def createFactory(aggregationName: String, config: ValuesSourceConfig[Numeric]): AggregatorFactory = {
    new CountDistinctAggregatorFactory(aggregationName, config)
  }
}
