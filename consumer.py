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
    """Simplified batch processor with better error handling"""
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
                logger.info(f"Processing row {i+1}: {list(row_dict.keys())}")
                
                # Create timestamp
                timestamp = None
                if 'event_time' in row_dict and row_dict['event_time']:
                    timestamp = row_dict['event_time']
                elif 'sensor_timestamp' in row_dict and row_dict['sensor_timestamp']:
                    timestamp = datetime.fromtimestamp(int(row_dict['sensor_timestamp']) / 1000)
                else:
                    timestamp = datetime.utcnow()
                
                logger.info(f"Using timestamp: {timestamp}")
                
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
                                logger.info(f"Added field {field_name}: {float_val}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not convert {field_name}={value}: {e}")
                
                if fields_added > 0:
                    points.append(point)
                    logger.info(f"Created point with {fields_added} fields")
                else:
                    logger.warning(f"No valid fields for row {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing row {i+1}: {e}")
                import traceback
                logger.error(traceback.format_exc())
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

def write_points_to_influxdb(points, measurement_name):
    """Write points to InfluxDB with detailed logging"""
    logger.info(f"Writing {len(points)} points to InfluxDB for {measurement_name}")
    
    try:
        # Create InfluxDB client
        client = InfluxDBClient(
            url=INFLUX_CONFIG['url'], 
            token=INFLUX_CONFIG['token'], 
            org=INFLUX_CONFIG['org']
        )
        
        # Test connection
        health = client.health()
        logger.info(f"InfluxDB health: {health.status}")
        
        # Create write API
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        # Write points
        write_api.write(bucket=INFLUX_CONFIG['bucket'], record=points)
        logger.info(f"✅ Successfully wrote {len(points)} points to {measurement_name}")
        
        # Close client
        client.close()
        
    except Exception as e:
        logger.error(f"❌ Error writing to InfluxDB: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Log sample point for debugging
        if points:
            logger.error(f"Sample point: {points[0]}")

class HydroponicAnalyticsConsumer:
    def __init__(self):
        # Prometheus metrics
        self.records_processed = Counter('records_processed_total', 'Total records processed')
        self.batches_written = Counter('batches_written_total', 'Total batches written to InfluxDB')
        self.write_errors = Counter('write_errors_total', 'Total InfluxDB write errors')
        self.active_streams = Gauge('active_streams', 'Number of active Spark streams')
        
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
        logger.info("SIMPLIFIED PROCESSING MODE")
        logger.info("Only processing raw sensor data for now")
        logger.info("=" * 60)
        
        try:
            # Read and parse data
            raw_stream = self.read_kafka_stream()
            parsed_stream = self.parse_json_data(raw_stream)
            
            # Start ONLY the raw data stream for now
            raw_query = parsed_stream.writeStream \
                .foreachBatch(lambda df, batch_id: simple_batch_processor(df, batch_id, "raw_sensor_data")) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/checkpoints/raw_simplified") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            self.active_queries.append(raw_query)
            self.active_streams.set(len(self.active_queries))
            
            logger.info(f"✅ Started {len(self.active_queries)} streaming query")
            logger.info("📊 Monitor logs for data processing...")
            
            # Wait for the query
            raw_query.awaitTermination()
                
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