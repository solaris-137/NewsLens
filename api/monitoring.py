import logging
import os

from opencensus.ext.azure import metrics_exporter
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats import view as view_module
from opencensus.tags import tag_map as tag_map_module

logger = logging.getLogger(__name__)

_exporter = None
_views_registered = False
_stats = stats_module.stats


articles_processed_measure = measure_module.MeasureInt(
    "articles_processed",
    "Number of articles processed",
    "1",
)
articles_filtered_measure = measure_module.MeasureInt(
    "articles_filtered",
    "Number of articles filtered out",
    "1",
)
avg_sentiment_measure = measure_module.MeasureFloat(
    "avg_sentiment",
    "Average composite sentiment score",
    "1",
)
pipeline_latency_measure = measure_module.MeasureFloat(
    "pipeline_latency",
    "NLP pipeline latency",
    "ms",
)


def init_app_insights():
    global _exporter

    if _exporter is not None:
        return

    conn_str = os.environ.get("AZURE_APPINSIGHTS_CONN_STR")
    if not conn_str:
        logger.info("AZURE_APPINSIGHTS_CONN_STR not set - App Insights disabled")
        return

    _exporter = metrics_exporter.new_metrics_exporter(
        connection_string=conn_str,
        export_interval=60,
        enable_standard_metrics=False,
    )
    _stats.view_manager.register_exporter(_exporter)


def register_views():
    global _views_registered

    if _views_registered:
        return

    articles_processed_view = view_module.View(
        "articles_processed_total",
        "Total articles processed",
        [],
        articles_processed_measure,
        aggregation_module.CountAggregation(),
    )
    articles_filtered_view = view_module.View(
        "articles_filtered_total",
        "Total articles filtered",
        [],
        articles_filtered_measure,
        aggregation_module.CountAggregation(),
    )
    avg_sentiment_view = view_module.View(
        "avg_sentiment_24hr",
        "Average sentiment last 24hr",
        [],
        avg_sentiment_measure,
        aggregation_module.LastValueAggregation(),
    )
    pipeline_latency_view = view_module.View(
        "pipeline_latency_ms",
        "Pipeline latency in ms",
        [],
        pipeline_latency_measure,
        aggregation_module.LastValueAggregation(),
    )

    _stats.view_manager.register_view(articles_processed_view)
    _stats.view_manager.register_view(articles_filtered_view)
    _stats.view_manager.register_view(avg_sentiment_view)
    _stats.view_manager.register_view(pipeline_latency_view)
    _views_registered = True


def record_metric(measure, value):
    if _exporter is None:
        return

    try:
        measurement_map = _stats.stats_recorder.new_measurement_map()
        tag_map = tag_map_module.TagMap()
        if isinstance(measure, measure_module.MeasureInt):
            measurement_map.measure_int_put(measure, int(value))
        else:
            measurement_map.measure_float_put(measure, float(value))
        measurement_map.record(tag_map)
    except Exception as exc:
        logger.warning(f"App Insights metric record failed | {exc}")
