import time
import math
import random
import json
import threading
from confluent_kafka import Producer
from datetime import datetime
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealisticHydroponicSimulator:
    def __init__(self):
        # Kafka producer configuration with better settings
        self.producer = Producer({
            'bootstrap.servers': 'kafka:9092',
            'client.id': 'hydro-sensor-simulator',
            'batch.size': 16384,
            'linger.ms': 10,
            'compression.type': 'snappy',
            'retries': 3,
            'retry.backoff.ms': 100
        })
        
        # Prometheus metrics
        self.messages_sent = Counter('messages_sent_total', 'Total messages sent')
        self.send_errors = Counter('send_errors_total', 'Total send errors')
        self.water_temp_gauge = Gauge('water_temperature', 'Water temperature')
        self.ph_gauge = Gauge('water_ph', 'Water pH')
        self.ec_gauge = Gauge('water_ec', 'Water EC')
        self.water_level_gauge = Gauge('water_level_percent', 'Water level percentage')
        
        # Physical state variables
        self.water_temp = 21.0
        self.ph = 6.0
        self.ec = 1.6
        self.water_level = 85.0
        self.air_temp = 24.0
        self.humidity = 60.0
        self.light_intensity = 0
        
        # System automation states
        self.last_refill_time = time.time()
        self.last_ph_adjust_time = time.time()
        self.last_nutrient_dose_time = time.time()
        
        # Sensor calibration drift
        self.ph_drift = 0.0
        self.ec_drift = 0.0
        self.temp_offset = 0.0
        
        # Plant uptake rates
        self.base_water_uptake = 0.5
        self.base_nutrient_uptake = 0.02
        
        # System parameters
        self.reservoir_volume = 100.0
        self.evaporation_rate = 0.1
        
        # Running flag
        self.running = False
        
    def get_time_factor(self):
        """Get time-based factor for daily cycles (0-1)"""
        current_hour = datetime.now().hour
        return (math.sin(2 * math.pi * (current_hour - 6) / 24) + 1) / 2
    
    def get_light_schedule(self):
        """Realistic light schedule with gradual sunrise/sunset"""
        hour = datetime.now().hour
        if 6 <= hour < 8:
            intensity = 800 * (hour - 6) / 2
        elif 8 <= hour < 20:
            intensity = 800 + 200 * math.sin(2 * math.pi * (hour - 8) / 12)
        elif 20 <= hour < 22:
            intensity = 800 * (22 - hour) / 2
        else:
            intensity = 0
        
        return max(0, intensity + random.gauss(0, 20))
    
    def calculate_plant_activity(self):
        """Calculate plant metabolic activity based on conditions"""
        light_factor = min(1.0, self.light_intensity / 800.0)
        temp_factor = max(0.1, 1.0 - abs(self.water_temp - 22.0) / 10.0)
        ph_factor = max(0.3, 1.0 - abs(self.ph - 6.0) / 1.5)
        
        return light_factor * temp_factor * ph_factor
    
    def simulate_automation(self, dt):
        """Simulate automated system responses"""
        current_time = time.time()
        
        # Auto water refill
        if self.water_level < 30.0 and current_time - self.last_refill_time > 300:
            refill_amount = random.uniform(40, 60)
            dilution_factor = refill_amount / (self.water_level + refill_amount)
            self.ec *= (1 - dilution_factor * 0.8)
            self.ph += dilution_factor * 0.3
            self.water_level += refill_amount
            self.water_level = min(100.0, self.water_level)
            self.last_refill_time = current_time
            logger.info(f"Auto refill: +{refill_amount:.1f}% (EC diluted to {self.ec:.2f})")
        
        # Auto pH adjustment
        if (self.ph < 5.8 or self.ph > 6.2) and current_time - self.last_ph_adjust_time > 600:
            if self.ph < 5.8:
                self.ph += random.uniform(0.2, 0.4)
                logger.info(f"pH adjustment: UP to {self.ph:.2f}")
            else:
                self.ph -= random.uniform(0.2, 0.4)
                logger.info(f"pH adjustment: DOWN to {self.ph:.2f}")
            self.last_ph_adjust_time = current_time
        
        # Auto nutrient dosing
        if self.ec < 1.3 and current_time - self.last_nutrient_dose_time > 900:
            dose_amount = random.uniform(0.2, 0.4)
            self.ec += dose_amount
            self.ph -= dose_amount * 0.1
            self.last_nutrient_dose_time = current_time
            logger.info(f"Nutrient dose: +{dose_amount:.2f} EC (pH adjusted to {self.ph:.2f})")
    
    def simulate_sensor_errors(self):
        """Simulate realistic sensor issues"""
        if random.random() < 0.01:
            error_type = random.choice(['ph_spike', 'ec_spike', 'temp_dropout'])
            
            if error_type == 'ph_spike':
                self.ph += random.uniform(-2.0, 2.0)
                logger.warning(f"pH sensor spike: {self.ph:.2f}")
            elif error_type == 'ec_spike':
                self.ec *= random.uniform(0.5, 2.0)
                logger.warning(f"EC sensor spike: {self.ec:.2f}")
            elif error_type == 'temp_dropout':
                self.water_temp = float('nan')
                logger.warning("Temperature sensor dropout")
        
        # Gradual calibration drift
        self.ph_drift += random.gauss(0, 0.0001)
        self.ec_drift += random.gauss(0, 0.0001)
        self.temp_offset += random.gauss(0, 0.0001)
    
    def simulate_physics(self, dt):
        """Simulate physical processes"""
        self.light_intensity = self.get_light_schedule()
        plant_activity = self.calculate_plant_activity()
        
        # Water and evaporation
        water_uptake_rate = self.base_water_uptake * plant_activity * dt / 3600
        evaporation = self.evaporation_rate * (1 + (self.air_temp - 20) / 20) * dt / 3600
        self.water_level -= (water_uptake_rate + evaporation)
        self.water_level = max(0.0, self.water_level)
        
        # Nutrient uptake and concentration
        nutrient_uptake = self.base_nutrient_uptake * plant_activity * dt / 3600
        if self.water_level > 10:
            concentration_factor = 1 + (evaporation / max(0.1, self.water_level / 100))
            self.ec = (self.ec * concentration_factor) - nutrient_uptake
            self.ec = max(0.5, self.ec)
        
        # Temperature dynamics
        time_factor = self.get_time_factor()
        outdoor_temp = 22 + 6 * time_factor + random.gauss(0, 0.5)
        equipment_heat = 2.0 if self.light_intensity > 100 else 0.5
        self.air_temp += (outdoor_temp + equipment_heat - self.air_temp) * 0.1 * dt
        self.water_temp += (self.air_temp - self.water_temp) * 0.03 * dt
        
        # Humidity dynamics
        base_humidity = 70 - (self.air_temp - 20) * 2
        evap_humidity = evaporation * 10
        target_humidity = base_humidity + evap_humidity + random.gauss(0, 2)
        self.humidity += (target_humidity - self.humidity) * 0.2 * dt
        self.humidity = max(40.0, min(85.0, self.humidity))
        
        # pH buffering and EC-pH interaction
        ec_ph_effect = (self.ec - 1.6) * 0.1
        ph_buffer_strength = 0.95
        natural_ph_drift = random.gauss(0, 0.01) - ec_ph_effect
        self.ph += natural_ph_drift * (1 - ph_buffer_strength) * dt
        
        # Bounds checking
        self.water_temp = max(15.0, min(30.0, self.water_temp))
        self.ph = max(4.0, min(8.0, self.ph))
        self.ec = max(0.3, min(3.0, self.ec))
        
    def get_sensor_reading(self, true_value, noise_std, drift=0.0):
        """Apply sensor noise and calibration drift"""
        if math.isnan(true_value):
            return None
        
        reading = true_value + drift + random.gauss(0, noise_std)
        return reading
    
    def simulate_sensors(self):
        """Generate realistic sensor readings"""
        dt = 1.0
        
        try:
            self.simulate_automation(dt)
            self.simulate_physics(dt)
            self.simulate_sensor_errors()
        except Exception as e:
            logger.error(f"Error in simulation: {e}")
            return None
        
        readings = {
            'water_temperature': self.get_sensor_reading(self.water_temp, 0.1, self.temp_offset),
            'water_ph': self.get_sensor_reading(self.ph, 0.02, self.ph_drift),
            'ec_tds': self.get_sensor_reading(self.ec, 0.01, self.ec_drift),
            'water_level_percent': self.get_sensor_reading(self.water_level, 0.5),
            'air_temperature': self.get_sensor_reading(self.air_temp, 0.2),
            'humidity': self.get_sensor_reading(self.humidity, 1.0),
            'light_intensity_ppfd': self.get_sensor_reading(self.light_intensity, 10.0)
        }
        
        # Round values
        for key, value in readings.items():
            if value is not None:
                readings[key] = round(value, 2 if 'ph' in key or 'ec' in key else 1)
        
        # Update Prometheus metrics
        try:
            if readings['water_temperature'] is not None:
                self.water_temp_gauge.set(readings['water_temperature'])
            if readings['water_ph'] is not None:
                self.ph_gauge.set(readings['water_ph'])
            if readings['ec_tds'] is not None:
                self.ec_gauge.set(readings['ec_tds'])
            if readings['water_level_percent'] is not None:
                self.water_level_gauge.set(readings['water_level_percent'])
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
        
        return {
            'timestamp': int(time.time() * 1000),
            'sensors': readings,
            'system_state': {
                'plant_activity': round(self.calculate_plant_activity(), 3),
                'last_refill': int(time.time() - self.last_refill_time),
                'last_ph_adjust': int(time.time() - self.last_ph_adjust_time),
                'last_nutrient_dose': int(time.time() - self.last_nutrient_dose_time)
            }
        }
    
    def delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
            self.send_errors.inc()
        else:
            self.messages_sent.inc()
    
    def run(self):
        """Main simulation loop"""
        logger.info("Starting hydroponic sensor simulation...")
        logger.info("Starting Prometheus metrics server on port 8000...")
        
        # Start Prometheus metrics server
        try:
            start_http_server(8000)
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            return
        
        self.running = True
        consecutive_errors = 0
        
        try:
            while self.running:
                # Generate sensor data
                sensor_data = self.simulate_sensors()
                
                if sensor_data is None:
                    consecutive_errors += 1
                    if consecutive_errors > 10:
                        logger.error("Too many consecutive errors, stopping")
                        break
                    time.sleep(1.0)
                    continue
                
                consecutive_errors = 0
                
                # Send to Kafka with retry logic
                try:
                    self.producer.produce(
                        topic='hydrop-sensors',
                        key='hydro_system_01',
                        value=json.dumps(sensor_data),
                        callback=self.delivery_report
                    )
                    
                    # Flush every 10 messages or on timeout
                    self.producer.poll(0)
                    
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
                    self.send_errors.inc()
                
                # Print current readings
                sensors = sensor_data['sensors']
                state = sensor_data['system_state']
                
                def safe_display(value, unit=''):
                    return f"{value}{unit}" if value is not None else "ERROR"
                
                logger.info(f"Water: {safe_display(sensors['water_temperature'], '°C')}, "
                          f"pH: {safe_display(sensors['water_ph'])}, "
                          f"EC: {safe_display(sensors['ec_tds'], ' mS/cm')}, "
                          f"Level: {safe_display(sensors['water_level_percent'], '%')}, "
                          f"Activity: {state['plant_activity']:.2f}")
                
                time.sleep(1.0)
                
        except KeyboardInterrupt:
            logger.info("Shutting down simulator...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.running = False
            try:
                self.producer.flush(timeout=5)
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

if __name__ == '__main__':
    simulator = RealisticHydroponicSimulator()
    simulator.run()