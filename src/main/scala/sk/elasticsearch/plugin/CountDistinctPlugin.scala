package sk.elasticsearch.plugin

import org.elasticsearch.plugins.AbstractPlugin
import org.elasticsearch.search.aggregations.AggregationModule
import sk.elasticsearch.aggregations.CountDistinctPluginParser

class CountDistinctPlugin extends AbstractPlugin {
  override def description(): String = "Count Distinct Plugin"

  override def name(): String = "countdistinct"

  def onModule(aggModule: AggregationModule ) {
    aggModule.addAggregatorParser(classOf[CountDistinctPluginParser])
  }
}
