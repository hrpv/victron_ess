#!/usr/bin/env python3
"""
PV Inverter MQTT to DBus Bridge - Refactored Version

Fetches PV values via MQTT from ehzlogger_v3 and exposes them on DBus.
- Total power: ehzmeter/pvpower
- Total energy: ehzmeter/pvtotal
- Per-phase power (L1/L2/L3): ehzmeter/pvpwrl123
"""

import os
import sys
import json
import signal
import logging
import platform
import time
from threading import Lock, Timer
from typing import Dict, Optional

import paho.mqtt.client as mqtt

try:
    import gobject  # Python 2.x
except ImportError:
    from gi.repository import GLib as gobject  # Python 3.x

# Add path for Victron modules
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '/opt/victronenergy/dbus-systemcalc-py/ext/velib_python'))
from vedbus import VeDbusService

# ============================================================================
# Configuration
# ============================================================================

CONFIG = {
    'broker_address': os.getenv('MQTT_BROKER', '192.168.178.58'),
    'broker_port': int(os.getenv('MQTT_PORT', 1883)),
    'client_name': 'ehzmeter',
    'initial_energy_offset': 73311,  # kWh offset at 12.09.23
    'update_interval': 10000,  # 10 seconds in milliseconds
    'reconnect_delay': 5,  # Initial reconnect delay in seconds
    'max_reconnect_delay': 300,  # Max reconnect delay (5 minutes)
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'dbus_service': 'com.victronenergy.pvinverter.pv0',
    'device_instance': 41,
    'product_name': 'SMAL1 HM L1 L2 L3',
}

# ============================================================================
# Logging Setup
# ============================================================================

logging.basicConfig(
    level=getattr(logging, CONFIG['log_level']),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# MQTT Data Store (Thread-Safe)
# ============================================================================

class MQTTDataStore:
    """Thread-safe storage for MQTT data."""
    
    def __init__(self):
        self._lock = Lock()
        self.pow_ges = 0.0          # Total power (W)
        self.e_today = 0.0          # Energy today (0.1 Wh units)
        self.e_total = CONFIG['initial_energy_offset'] * 10000  # Total energy (0.1 Wh units)
        self.pow_l1 = 0.0           # Phase 1 power (W)
        self.pow_l2 = 0.0           # Phase 2 power (W)
        self.pow_l3 = 0.0           # Phase 3 power (W)
        self.connected = False      # Connection status
    
    def update_power(self, value: float) -> None:
        """Update total power."""
        with self._lock:
            self.pow_ges = float(value)
            logger.debug(f"Updated total power: {self.pow_ges} W")
    
    def update_energy_today(self, value: float) -> None:
        """Update today's energy."""
        with self._lock:
            self.e_today = float(value)
            logger.debug(f"Updated energy today: {self.e_today}")
    
    def update_energy_total(self, value: float) -> None:
        """Update total energy."""
        with self._lock:
            self.e_total = float(value)
            logger.debug(f"Updated total energy: {self.e_total}")
    
    def update_phase_powers(self, l1: float, l2: float, l3: float) -> None:
        """Update phase powers."""
        with self._lock:
            self.pow_l1 = float(l1)
            self.pow_l2 = float(l2)
            self.pow_l3 = float(l3)
            logger.debug(f"Updated phase powers: L1={self.pow_l1}W, L2={self.pow_l2}W, L3={self.pow_l3}W")
    
    def set_connected(self, state: bool) -> None:
        """Update connection status."""
        with self._lock:
            self.connected = state
            logger.info(f"MQTT connection status: {state}")
    
    def get_all(self) -> Dict:
        """Get all values as a snapshot."""
        with self._lock:
            return {
                'pow_ges': self.pow_ges,
                'e_today': self.e_today,
                'e_total': self.e_total,
                'pow_l1': self.pow_l1,
                'pow_l2': self.pow_l2,
                'pow_l3': self.pow_l3,
                'connected': self.connected,
            }


# ============================================================================
# MQTT Client
# ============================================================================

class MQTTClient:
    """Manages MQTT connection and message handling."""
    
    def __init__(self, data_store: MQTTDataStore):
        self.data_store = data_store
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, CONFIG['client_name'])
        self.reconnect_delay = CONFIG['reconnect_delay']
        self.reconnect_timer: Optional[Timer] = None
        
        # Register callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
    
    def connect(self) -> None:
        """Connect to MQTT broker."""
        try:
            logger.info(f"Connecting to MQTT broker at {CONFIG['broker_address']}:{CONFIG['broker_port']}")
            self.client.connect(CONFIG['broker_address'], CONFIG['broker_port'], keepalive=60)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self._schedule_reconnect()
    
    def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        logger.info("Disconnecting from MQTT broker")
        if self.reconnect_timer:
            self.reconnect_timer.cancel()
        self.client.loop_stop()
        self.client.disconnect()
    
    def _on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker successfully")
            self.data_store.set_connected(True)
            self.reconnect_delay = CONFIG['reconnect_delay']  # Reset delay on successful connection
            client.subscribe("ehzmeter/#")
            logger.info("Subscribed to ehzmeter/# topics")
        else:
            logger.error(f"Failed to connect to MQTT broker (rc={rc})")
            self.data_store.set_connected(False)
    
    def _on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection."""
        self.data_store.set_connected(False)
        if rc == 0:
            logger.info("Disconnected from MQTT broker (gracefully)")
        else:
            logger.warning(f"Unexpected MQTT disconnection (rc={rc}). Will attempt to reconnect...")
            self._schedule_reconnect()
    
    def _on_message(self, client, userdata, message):
        """Handle incoming MQTT message."""
        try:
            msg = str(message.payload.decode("utf-8")).strip()
            topic = message.topic
            
            logger.debug(f"Received message on {topic}: {msg}")
            
            if topic == "ehzmeter/pvpower":
                self.data_store.update_power(msg)
            
            elif topic == "ehzmeter/pvtoday":
                self.data_store.update_energy_today(msg)
            
            elif topic == "ehzmeter/pvtotal":
                self.data_store.update_energy_total(msg)
            
            elif topic == "ehzmeter/pvpwrl123":
                try:
                    parts = msg.split(",")
                    if len(parts) != 3:
                        logger.error(f"Invalid pvpwrl123 format: {msg}. Expected 3 comma-separated values.")
                        return
                    self.data_store.update_phase_powers(parts[0], parts[1], parts[2])
                except ValueError as e:
                    logger.error(f"Failed to parse phase powers: {msg}. Error: {e}")
        
        except Exception as e:
            logger.error(f"Error processing MQTT message on {message.topic}: {e}")
    
    def _schedule_reconnect(self) -> None:
        """Schedule reconnection attempt with exponential backoff."""
        if self.reconnect_timer:
            self.reconnect_timer.cancel()
        
        delay = min(self.reconnect_delay, CONFIG['max_reconnect_delay'])
        logger.info(f"Scheduling reconnection in {delay} seconds...")
        self.reconnect_timer = Timer(delay, self.connect)
        self.reconnect_timer.daemon = True
        self.reconnect_timer.start()
        
        # Increase delay for next attempt (exponential backoff)
        self.reconnect_delay = min(self.reconnect_delay * 1.5, CONFIG['max_reconnect_delay'])


# ============================================================================
# DBus PV Inverter Service
# ============================================================================

class DbusPVInverter:
    """DBus service for PV Inverter data."""
    
    def __init__(self, data_store: MQTTDataStore):
        self.data_store = data_store
        self._dbusservice = VeDbusService(CONFIG['dbus_service'])
        self._setup_paths()
        
        # Schedule periodic updates
        gobject.timeout_add(CONFIG['update_interval'], self._update)
        
        logger.info(f"DBus service '{CONFIG['dbus_service']}' initialized")
    
    def _setup_paths(self) -> None:
        """Set up DBus paths."""
        self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
        self._dbusservice.add_path('/Mgmt/ProcessVersion', f'Refactored v1.0, Python {platform.python_version()}')
        self._dbusservice.add_path('/Mgmt/Connection', 'MQTT')
        
        self._dbusservice.add_path('/DeviceInstance', CONFIG['device_instance'])
        self._dbusservice.add_path('/ProductId', 0xFFFF)
        self._dbusservice.add_path('/ProductName', CONFIG['product_name'])
        self._dbusservice.add_path('/Position', 0)
        self._dbusservice.add_path('/FirmwareVersion', '1.0')
        self._dbusservice.add_path('/HardwareVersion', '0')
        self._dbusservice.add_path('/Connected', 1)
        
        # AC Power paths
        self._dbusservice.add_path('/Ac/Power', 0, writeable=False)
        self._dbusservice.add_path('/Ac/Energy/Forward', 0, writeable=False)
        
        # Per-phase paths
        for phase in ['L1', 'L2', 'L3']:
            self._dbusservice.add_path(f'/Ac/{phase}/Voltage', 230, writeable=False)
            self._dbusservice.add_path(f'/Ac/{phase}/Current', 0, writeable=False)
            self._dbusservice.add_path(f'/Ac/{phase}/Power', 0, writeable=False)
            self._dbusservice.add_path(f'/Ac/{phase}/Energy/Forward', 0, writeable=False)
        
        # Update index
        self._dbusservice.add_path('/UpdateIndex', 0, writeable=False)
    
    def _update(self) -> bool:
        """Update all DBus paths with current data."""
        try:
            data = self.data_store.get_all()
            
            # Convert total energy from 0.1 Wh to kWh
            e_totkwh = (data['e_total'] / 10000.0) - CONFIG['initial_energy_offset']
            
            # Update total power and energy
            self._dbusservice['/Ac/Power'] = round(data['pow_ges'], 1)
            self._dbusservice['/Ac/Energy/Forward'] = round(e_totkwh, 1)
            
            # Update per-phase data
            powers = {
                'L1': data['pow_l1'],
                'L2': data['pow_l2'],
                'L3': data['pow_l3'],
            }
            
            # Energy distribution percentages (customize as needed)
            energy_dist = {
                'L1': 0.576,
                'L2': 0.212,
                'L3': 0.212,
            }
            
            for phase, power in powers.items():
                current = round(power / 230.0, 1)  # Calculate current (P = V*I, V=230)
                self._dbusservice[f'/Ac/{phase}/Current'] = current
                self._dbusservice[f'/Ac/{phase}/Power'] = round(power, 1)
                self._dbusservice[f'/Ac/{phase}/Energy/Forward'] = round(e_totkwh * energy_dist[phase], 1)
            
            # Increment update index
            index = (self._dbusservice['/UpdateIndex'] + 1) % 256
            self._dbusservice['/UpdateIndex'] = index
            
            logger.info(f"Updated DBus: Total Power={data['pow_ges']}W, Energy={e_totkwh:.1f}kWh, Connected={data['connected']}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error updating DBus paths: {e}")
            return True  # Continue updating


# ============================================================================
# Main Application
# ============================================================================

class Application:
    """Main application coordinator."""
    
    def __init__(self):
        self.data_store = MQTTDataStore()
        self.mqtt_client = MQTTClient(self.data_store)
        self.dbus_inverter = None
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def start(self) -> None:
        """Start the application."""
        logger.info("Starting PV Inverter MQTT Bridge...")
        
        try:
            # Setup DBus
            from dbus.mainloop.glib import DBusGMainLoop
            DBusGMainLoop(set_as_default=True)
            
            self.dbus_inverter = DbusPVInverter(self.data_store)
            
            # Connect to MQTT
            self.mqtt_client.connect()
            
            logger.info("Application started successfully. Entering main loop...")
            mainloop = gobject.MainLoop()
            mainloop.run()
        
        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            self.shutdown()
            sys.exit(1)
    
    def shutdown(self) -> None:
        """Gracefully shutdown the application."""
        logger.info("Shutting down application...")
        self.mqtt_client.disconnect()
        logger.info("Shutdown complete")
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}. Initiating shutdown...")
        self.shutdown()
        sys.exit(0)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    app = Application()
    app.start()

