package sk.elasticsearch.aggregations

import org.apache.lucene.index.AtomicReaderContext
import org.elasticsearch.common.io.stream.{StreamInput, StreamOutput}
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentParser}
import org.elasticsearch.index.fielddata.SortedBinaryDocValues
import org.elasticsearch.search.SearchParseException
import org.elasticsearch.search.aggregations.InternalAggregation.{CommonFields, ReduceContext, Type}
import org.elasticsearch.search.aggregations.metrics.{InternalNumericMetricsAggregation, NumericMetricsAggregator}
import org.elasticsearch.search.aggregations.support._
import org.elasticsearch.search.aggregations.{Aggregator, AggregatorFactory, InternalAggregation}
import org.elasticsearch.search.internal.SearchContext

import scala.collection.JavaConversions._
import scala.collection.mutable

object InternalCountDistinct {
  val TYPE: Type = new Type("countdistinct")
}

sealed case class InternalCountDistinctS (var set: mutable.HashSet[String], var nam: String) extends InternalNumericMetricsAggregation.SingleValue(nam) {

  def this() = this(null, null)

  override def `type`(): Type = InternalCountDistinct.TYPE

  override def reduce(reduceContext: ReduceContext): InternalAggregation = {
    val set = reduceContext.aggregations().foldLeft(new mutable.HashSet[String]()) { case (s, obj) =>
      s ++ obj.asInstanceOf[InternalCountDistinctS].set
    }
    InternalCountDistinctS(set, name)
  }

  override def doXContentBody(builder: XContentBuilder, params: Params): XContentBuilder = {
    builder.field(CommonFields.VALUE, set.size)
    builder
  }

  override def writeTo(out: StreamOutput): Unit = {
    out.writeString(nam)
    out.writeStringArray(set.toArray)
  }

  override def readFrom(in: StreamInput): Unit = {
    nam = in.readString()
    name = nam
    set = new mutable.HashSet[String] () ++ in.readStringArray()
  }

  override def value(): Double = set.size
}

class CountDistinctSAggregator (nam: String,
                    estimatedBucketsCount: Long,
                    aggregationContext: AggregationContext,
                    parent: Aggregator,
                    valuesSource: Option[ValuesSource])
  extends NumericMetricsAggregator.SingleValue(nam, estimatedBucketsCount, aggregationContext, parent) {

  private var values: SortedBinaryDocValues = _

  private val ord2set = mutable.Map[Long, mutable.HashSet[String]] ()

  override def shouldCollect(): Boolean = valuesSource.isDefined

  override def buildEmptyAggregation(): InternalAggregation = {
    InternalCountDistinctS(new mutable.HashSet[String](), name)
  }

  override def buildAggregation(owningBucketOrdinal: Long): InternalAggregation = {
    ord2set.get(owningBucketOrdinal) match {
      case Some(set) => InternalCountDistinctS(set, name)
      case _ => buildEmptyAggregation()
    }
  }

  override def collect(docId: Int, bucketOrdinal: Long): Unit = {
    values.setDocument(docId)
    val list = mutable.ListBuffer[String]()
    for (i <- 0 until values.count()) {
      list += values.valueAt(i).utf8ToString()
    }
    val newSet = ord2set.get(bucketOrdinal) match {
      case Some(set) => set ++ list
      case _ => new mutable.HashSet[String]() ++ list
    }

    ord2set += (bucketOrdinal -> newSet)
  }

  override def setNextReader(reader: AtomicReaderContext): Unit = {
    values = valuesSource.get.bytesValues()
  }

  override def metric(owningBucketOrd: Long): Double = {
    valuesSource.isDefined match {
      case true => ord2set(owningBucketOrd).size
      case false => Double.NaN
    }
  }
}

class CountDistinctAggregatorFactory(nam: String, config: ValuesSourceConfig[ValuesSource])
  extends ValuesSourceAggregatorFactory[ValuesSource](nam, InternalCountDistinct.TYPE.name(), config) {

  override def createUnmapped(aggregationContext: AggregationContext,
                              parent: Aggregator): Aggregator = {
    new CountDistinctSAggregator(nam, 0, aggregationContext, parent, None)
  }

  override def create(valuesSource: ValuesSource,
                      expectedBucketsCount: Long,
                      aggregationContext: AggregationContext,
                      parent: Aggregator): Aggregator = {
    valuesSource match {
      case numeric: ValuesSource.Numeric => new CountDistinctNAggregator(nam, expectedBucketsCount, aggregationContext, parent, Some(numeric))
      case bytes: ValuesSource.Bytes => new CountDistinctSAggregator(nam, expectedBucketsCount, aggregationContext, parent, Some(bytes))
      case _ => null
    }
  }
}

class CountDistinctPluginParser extends Aggregator.Parser {
  override def `type`(): String = InternalCountDistinct.TYPE.name()

  override def parse(aggregationName: String, parser: XContentParser, context: SearchContext): AggregatorFactory = {
    val vsParser = ValuesSourceParser.any(aggregationName, InternalCountDistinct.TYPE, context)
      .formattable(false).build.asInstanceOf[ValuesSourceParser[ValuesSource]]

    var currentFieldName: String = null
    var done = false
    while (!done) {
      val token = parser.nextToken
      if (token != XContentParser.Token.END_OBJECT) {
        if (token eq XContentParser.Token.FIELD_NAME) {
          currentFieldName = parser.currentName
        } else if (vsParser.token(currentFieldName, token, parser)) {
        } else {
          throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].")
        }
      } else {
        done = true
      }
    }

    new CountDistinctAggregatorFactory(aggregationName, vsParser.config())
  }
}
