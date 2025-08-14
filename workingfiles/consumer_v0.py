from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
import numpy as np
from datetime import datetime, timedelta
import os
import findspark
from functools import reduce
from operator import add
from prometheus_client import Counter, Gauge, start_http_server
import logging
import threading
import time
import math

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force specific Spark/Scala versions
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 --conf spark.sql.streaming.schemaInference=true pyspark-shell'

# Initialize Spark
findspark.init()

# Global InfluxDB connection config
INFLUX_CONFIG = {
    'url': "http://influxdb:8086",
    'token': "hydro-super-secret-token-12345",
    'org': "hydroponic-org",
    'bucket': "hydroponic_data"
}

def test_influxdb_connection():
    """Test InfluxDB connection before starting"""
    try:
        logger.info("Testing InfluxDB connection...")
        client = InfluxDBClient(
            url=INFLUX_CONFIG['url'], 
            token=INFLUX_CONFIG['token'], 
            org=INFLUX_CONFIG['org']
        )
        health = client.health()
        logger.info(f"InfluxDB health check: {health.status}")
        
        # Test write capability
        point = Point("test_measurement") \
            .tag("test", "connection") \
            .field("value", 1.0) \
            .time(datetime.utcnow())
        
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=INFLUX_CONFIG['bucket'], record=point)
        logger.info("InfluxDB test write successful")
        
        client.close()
        return True
    except Exception as e:
        logger.error(f"InfluxDB connection test failed: {e}")
        return False

def simple_batch_processor(batch_df, batch_id, measurement_name):
    """Simplified batch processor for raw sensor data"""
    try:
        row_count = batch_df.count()
        logger.info(f"=== PROCESSING BATCH {batch_id} for {measurement_name} ===")
        logger.info(f"Batch has {row_count} rows")
        
        if row_count == 0:
            logger.warning(f"Empty batch for {measurement_name}")
            return
        
        # Collect all rows (for small batches this is fine)
        rows = batch_df.collect()
        logger.info(f"Collected {len(rows)} rows for processing")
        
        # Process rows directly
        points = []
        for i, row in enumerate(rows):
            try:
                row_dict = row.asDict()
                logger.debug(f"Processing row {i+1}: {list(row_dict.keys())}")
                
                # Create timestamp
                timestamp = None
                if 'event_time' in row_dict and row_dict['event_time']:
                    timestamp = row_dict['event_time']
                elif 'sensor_timestamp' in row_dict and row_dict['sensor_timestamp']:
                    timestamp = datetime.fromtimestamp(int(row_dict['sensor_timestamp']) / 1000)
                else:
                    timestamp = datetime.utcnow()
                
                # Create InfluxDB point
                point = Point(measurement_name) \
                    .time(timestamp) \
                    .tag("system_id", "hydro_system_01")
                
                # Add sensor fields
                sensor_fields = [
                    'water_temperature', 'water_ph', 'ec_tds', 'water_level_percent',
                    'air_temperature', 'humidity', 'light_intensity_ppfd', 'plant_activity',
                    'last_refill', 'last_ph_adjust', 'last_nutrient_dose'
                ]
                
                fields_added = 0
                for field_name in sensor_fields:
                    if field_name in row_dict and row_dict[field_name] is not None:
                        value = row_dict[field_name]
                        try:
                            # Convert to float and check for valid number
                            float_val = float(value)
                            if not (math.isnan(float_val) or math.isinf(float_val)):
                                point = point.field(field_name, float_val)
                                fields_added += 1
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not convert {field_name}={value}: {e}")
                
                if fields_added > 0:
                    points.append(point)
                else:
                    logger.warning(f"No valid fields for row {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing row {i+1}: {e}")
                continue
        
        # Write points to InfluxDB
        if points:
            write_points_to_influxdb(points, measurement_name)
        else:
            logger.warning(f"No points to write for {measurement_name}")
            
        logger.info(f"=== COMPLETED BATCH {batch_id} ===")
        
    except Exception as e:
        logger.error(f"Error in batch processor {batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())

def aggregation_batch_processor(batch_df, batch_id, measurement_name):
    """Batch processor for aggregated data"""
    try:
        row_count = batch_df.count()
        logger.info(f"=== PROCESSING AGGREGATION BATCH {batch_id} for {measurement_name} ===")
        logger.info(f"Batch has {row_count} rows")
        
        if row_count == 0:
            logger.warning(f"Empty aggregation batch for {measurement_name}")
            return
        
        rows = batch_df.collect()
        points = []
        
        for i, row in enumerate(rows):
            try:
                row_dict = row.asDict()
                
                # Extract timestamp from window
                timestamp = None
                if 'window' in row_dict and row_dict['window']:
                    timestamp = row_dict['window']['start']
                else:
                    timestamp = datetime.utcnow()
                
                # Create InfluxDB point
                point = Point(measurement_name) \
                    .time(timestamp) \
                    .tag("system_id", "hydro_system_01")
                
                # Add analysis type and window duration as tags
                if 'analysis_type' in row_dict and row_dict['analysis_type']:
                    point = point.tag("analysis_type", str(row_dict['analysis_type']))
                if 'window_duration' in row_dict and row_dict['window_duration']:
                    point = point.tag("window_duration", str(row_dict['window_duration']))
                
                # Add all numeric fields
                exclude_fields = ['window', 'analysis_type', 'window_duration']
                fields_added = 0
                
                for key, value in row_dict.items():
                    if key not in exclude_fields and value is not None:
                        try:
                            if isinstance(value, (int, float)):
                                if not (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                                    point = point.field(key, float(value))
                                    fields_added += 1
                        except Exception as e:
                            logger.warning(f"Error processing aggregation field {key}={value}: {e}")
                
                if fields_added > 0:
                    points.append(point)
                    
            except Exception as e:
                logger.error(f"Error processing aggregation row {i+1}: {e}")
                continue
        
        # Write points to InfluxDB
        if points:
            write_points_to_influxdb(points, measurement_name)
            logger.info(f"✅ Wrote {len(points)} aggregation points to {measurement_name}")
        else:
            logger.warning(f"No aggregation points to write for {measurement_name}")
            
    except Exception as e:
        logger.error(f"Error in aggregation batch processor {batch_id}: {e}")

def write_points_to_influxdb(points, measurement_name):
    """Write points to InfluxDB with detailed logging"""
    logger.debug(f"Writing {len(points)} points to InfluxDB for {measurement_name}")
    
    try:
        # Create InfluxDB client
        client = InfluxDBClient(
            url=INFLUX_CONFIG['url'], 
            token=INFLUX_CONFIG['token'], 
            org=INFLUX_CONFIG['org']
        )
        
        # Create write API
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        # Write points
        write_api.write(bucket=INFLUX_CONFIG['bucket'], record=points)
        logger.info(f"✅ Successfully wrote {len(points)} points to {measurement_name}")
        
        # Close client
        client.close()
        
    except Exception as e:
        logger.error(f"❌ Error writing to InfluxDB: {e}")
        if points:
            logger.error(f"Sample point: {points[0]}")

class HydroponicAnalyticsConsumer:
    def __init__(self):
        # Prometheus metrics
        self.records_processed = Counter('records_processed_total', 'Total records processed')
        self.batches_written = Counter('batches_written_total', 'Total batches written to InfluxDB')
        self.write_errors = Counter('write_errors_total', 'Total InfluxDB write errors')
        self.active_streams = Gauge('active_streams', 'Number of active Spark streams')
        
        # Optimal ranges for anomaly detection
        self.optimal_ranges = {
            'water_temperature': (18.0, 25.0),
            'water_ph': (5.5, 6.5),
            'ec_tds': (1.2, 2.0),
            'water_level_percent': (25.0, 95.0),
            'air_temperature': (20.0, 28.0),
            'humidity': (50.0, 70.0),
            'light_intensity_ppfd': (0.0, 1200.0)
        }
        
        # Initialize Spark Session
        self.spark = None
        self._init_spark()
        
        # Track queries for cleanup
        self.active_queries = []
        
    def _init_spark(self):
        """Initialize Spark session"""
        try:
            self.spark = SparkSession.builder \
                .appName("HydroponicRealTimeAnalytics") \
                .master("local[*]") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.adaptive.enabled", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
        
    def create_schema(self):
        """Define schema for incoming JSON data"""
        return StructType([
            StructField("timestamp", LongType(), True),
            StructField("sensors", StructType([
                StructField("water_temperature", DoubleType(), True),
                StructField("water_ph", DoubleType(), True),
                StructField("ec_tds", DoubleType(), True),
                StructField("water_level_percent", DoubleType(), True),
                StructField("air_temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("light_intensity_ppfd", DoubleType(), True)
            ]), True),
            StructField("system_state", StructType([
                StructField("plant_activity", DoubleType(), True),
                StructField("last_refill", IntegerType(), True),
                StructField("last_ph_adjust", IntegerType(), True),
                StructField("last_nutrient_dose", IntegerType(), True)
            ]), True)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        logger.info("Setting up Kafka stream reader...")
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "hydrop-sensors") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_json_data(self, df):
        """Parse JSON data and extract sensor readings"""
        logger.info("Setting up JSON parsing...")
        
        schema = self.create_schema()
        
        parsed_df = df.select(
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("kafka_key"),
            col("data.timestamp").alias("sensor_timestamp"),
            col("data.sensors.*"),
            col("data.system_state.*")
        ).withColumn(
            "event_time", 
            to_timestamp(col("sensor_timestamp") / 1000)
        )
        
        logger.info(f"Parsed columns: {parsed_df.columns}")
        return parsed_df
    
    def calculate_basic_aggregations(self, df, window_duration):
        """Calculate basic aggregations for time windows"""
        sensor_cols = [
            'water_temperature', 'water_ph', 'ec_tds', 'water_level_percent',
            'air_temperature', 'humidity', 'light_intensity_ppfd', 'plant_activity'
        ]
        
        agg_exprs = []
        for col_name in sensor_cols:
            agg_exprs.extend([
                avg(col_name).alias(f"{col_name}_avg"),
                min(col_name).alias(f"{col_name}_min"),
                max(col_name).alias(f"{col_name}_max"),
                stddev(col_name).alias(f"{col_name}_stddev"),
                count(when(col(col_name).isNotNull(), 1)).alias(f"{col_name}_count")
            ])
        
        return df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), window_duration, window_duration)
            ) \
            .agg(*agg_exprs) \
            .withColumn("window_duration", lit(window_duration)) \
            .withColumn("analysis_type", lit("basic_aggregation"))
    
    def detect_anomalies(self, df, window_duration):
        """Detect anomalies using time-based windows"""
        anomaly_df = df
        
        for sensor, (min_val, max_val) in self.optimal_ranges.items():
            if sensor in df.columns:
                anomaly_df = anomaly_df.withColumn(
                    f"{sensor}_range_anomaly",
                    when((col(sensor) < min_val) | (col(sensor) > max_val), 1).otherwise(0)
                )
        
        anomaly_cols = [col for col in anomaly_df.columns if "_anomaly" in col]
        agg_exprs = [sum(col).alias(col) for col in anomaly_cols]
        agg_exprs.append(count("*").alias("total_readings"))
        
        return anomaly_df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(window(col("event_time"), window_duration)) \
            .agg(*agg_exprs) \
            .withColumn("analysis_type", lit("anomaly_detection"))
    
    def calculate_system_health(self, df, window_duration):
        """Calculate overall system health metrics"""
        health_df = df
        
        for sensor, (min_val, max_val) in self.optimal_ranges.items():
            if sensor in df.columns:
                optimal_center = (min_val + max_val) / 2
                optimal_range = max_val - min_val
                
                health_df = health_df.withColumn(
                    f"{sensor}_health_score",
                    greatest(lit(0), 
                        lit(1) - abs(col(sensor) - optimal_center) / (optimal_range / 2)
                    )
                )
        
        health_cols = [col for col in health_df.columns if "_health_score" in col]
        
        if health_cols:
            health_expr = reduce(add, [col(col_name) for col_name in health_cols]) / len(health_cols)
            health_df = health_df.withColumn("overall_system_health", health_expr)
        
        agg_exprs = [avg(col).alias(f"avg_{col}") for col in health_cols]
        agg_exprs.extend([
            avg("overall_system_health").alias("avg_system_health"),
            avg("plant_activity").alias("avg_plant_activity"),
            count("*").alias("total_readings")
        ])
        
        return health_df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), window_duration, window_duration)
            ) \
            .agg(*agg_exprs) \
            .withColumn("window_duration", lit(window_duration)) \
            .withColumn("analysis_type", lit("system_health"))
    
    def start_streaming(self):
        """Start the streaming analytics pipeline"""
        logger.info("🚀 Starting Hydroponic Real-time Analytics Consumer...")
        logger.info("Starting Prometheus metrics server on port 8001...")
        
        # Test InfluxDB connection first
        if not test_influxdb_connection():
            logger.error("❌ InfluxDB connection failed. Stopping.")
            return
        
        # Start metrics server
        try:
            start_http_server(8001)
            logger.info("✅ Prometheus metrics server started")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            return
        
        logger.info("=" * 60)
        logger.info("COMPLETE PROCESSING MODE")
        logger.info("Processing: Raw Data + Aggregations + Anomalies + Health")
        logger.info("Time Windows: 5min, 15min, 1hour")
        logger.info("=" * 60)
        
        try:
            # Read and parse data
            raw_stream = self.read_kafka_stream()
            parsed_stream = self.parse_json_data(raw_stream)
            
            # 1. Raw data stream (keep the working one)
            raw_query = parsed_stream.writeStream \
                .foreachBatch(lambda df, batch_id: simple_batch_processor(df, batch_id, "raw_sensor_data")) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/checkpoints/raw_simplified") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            self.active_queries.append(raw_query)
            logger.info("✅ Started raw data stream")
            
            # 2. Create all time-based analytics streams
            windows = ["5 minutes", "15 minutes", "1 hour"]
            
            for window in windows:
                window_safe = window.replace(" ", "_")
                
                # Basic Aggregations
                try:
                    agg_stream = self.calculate_basic_aggregations(parsed_stream, window)
                    agg_query = agg_stream.writeStream \
                        .foreachBatch(lambda df, batch_id, measurement=f"aggregations_{window_safe}": 
                                    aggregation_batch_processor(df, batch_id, measurement)) \
                        .outputMode("append") \
                        .option("checkpointLocation", f"/tmp/checkpoints/agg_{window_safe}") \
                        .trigger(processingTime='30 seconds') \
                        .start()
                    self.active_queries.append(agg_query)
                    logger.info(f"✅ Started aggregation stream for {window}")
                except Exception as e:
                    logger.error(f"Failed to start aggregation stream for {window}: {e}")
                
                # Anomaly Detection
                try:
                    anomaly_stream = self.detect_anomalies(parsed_stream, window)
                    anomaly_query = anomaly_stream.writeStream \
                        .foreachBatch(lambda df, batch_id, measurement=f"anomalies_{window_safe}": 
                                    aggregation_batch_processor(df, batch_id, measurement)) \
                        .outputMode("append") \
                        .option("checkpointLocation", f"/tmp/checkpoints/anomaly_{window_safe}") \
                        .trigger(processingTime='30 seconds') \
                        .start()
                    self.active_queries.append(anomaly_query)
                    logger.info(f"✅ Started anomaly detection stream for {window}")
                except Exception as e:
                    logger.error(f"Failed to start anomaly stream for {window}: {e}")
                
                # System Health
                try:
                    health_stream = self.calculate_system_health(parsed_stream, window)
                    health_query = health_stream.writeStream \
                        .foreachBatch(lambda df, batch_id, measurement=f"health_{window_safe}": 
                                    aggregation_batch_processor(df, batch_id, measurement)) \
                        .outputMode("append") \
                        .option("checkpointLocation", f"/tmp/checkpoints/health_{window_safe}") \
                        .trigger(processingTime='30 seconds') \
                        .start()
                    self.active_queries.append(health_query)
                    logger.info(f"✅ Started health monitoring stream for {window}")
                except Exception as e:
                    logger.error(f"Failed to start health stream for {window}: {e}")
            
            self.active_streams.set(len(self.active_queries))
            logger.info(f"🎉 Started {len(self.active_queries)} total streaming queries")
            logger.info("📊 All measurements will now be generated:")
            logger.info("   - raw_sensor_data (every 10s)")
            logger.info("   - aggregations_5_minutes, aggregations_15_minutes, aggregations_1_hour")
            logger.info("   - anomalies_5_minutes, anomalies_15_minutes, anomalies_1_hour")
            logger.info("   - health_5_minutes, health_15_minutes, health_1_hour")
            
            # Wait for all queries
            for query in self.active_queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("Shutting down streams...")
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        for query in reversed(self.active_queries):
            try:
                if query.isActive:
                    query.stop()
                    logger.info(f"Stopped query")
            except Exception as e:
                logger.error(f"Error stopping query: {e}")
        
        try:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark: {e}")

if __name__ == "__main__":
    consumer = HydroponicAnalyticsConsumer()
    consumer.start_streaming()