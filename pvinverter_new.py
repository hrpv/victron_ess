#!/usr/bin/env python
# pvwerte per MQTT von ehzlogger_v3 auf einen Raspberry PI abholen
# gesamtleistung ehzmeter/pvpower und gesamt energie ehzmeter/pvtotal 
#leistung aufgeteilt auf L1 L2 L3 ehzmeter/pvpwrl123 p1,p2,p3
try:
  import gobject  # Python 2.x
except:
  from gi.repository import GLib as gobject # Python 3.x
import platform
import logging
import time
import sys
import json
import os
import paho.mqtt.client as mqtt
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

# our own packages
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '/opt/victronenergy/dbus-systemcalc-py/ext/velib_python'))
from vedbus import VeDbusService

path_UpdateIndex = '/UpdateIndex'

# MQTT Setup
broker_address = "192.168.178.58"
port = 1883
MQTTNAME = "ehzmeter"

# Variablen setzen
verbunden = 0
durchlauf = 0
volt_ges = 0
volt_p1 = 0
volt_p2 = 0
volt_p3 = 0
amp_p1 = 0
amp_p2 = 0
amp_p3 = 0
pow_ges = 0
e_today = 0
e_total = 73311*10000  # 0.1 Wh initial start offset at 12.09. if ehzmeter update is not working
e_totkwh = 0
pow_l1 = 0
pow_l2 = 0
pow_l3 = 0

# MQTT Abfragen:

def on_disconnect(client, userdata, rc):
    global verbunden
    print("Client Got Disconnected")
    if rc != 0:
        print('Unexpected MQTT disconnection. Will auto-reconnect')

    else:
        print('rc value:' + str(rc))

    try:
        print("Trying to Reconnect")
        client.connect(broker_address)
        verbunden = 1
    except Exception as e:
        logging.exception("Fehler beim reconnecten mit Broker")
        print("Error in Retrying to Connect with Broker")
        verbunden = 0
        print(e)
            
def on_connect(client, userdata, flags, rc):
    print("Verbunden mit ehzmeter: " + broker_address)
    client.subscribe("ehzmeter/#")


def on_message(client, userdata, message):
#wichtig global, sonst werden die globalen variablen nicht aktualisiert
    global volt_ges,volt_p1, volt_p2, volt_p3, amp_p1, amp_p2, amp_p3, pow_ges, e_total, e_today,pow_l1,pow_l2,pow_l3

    msg = str(message.payload.decode("utf-8"))
    print("message received: ", msg)
    print("message topic: ", message.topic)
    if message.topic == "ehzmeter/pvpower":
        pow_ges = float(msg)   
    elif message.topic == "ehzmeter/pvtoday":
        e_today = float(msg)   # value is 0.1 Wh, divide by 10000 for kwh
    elif message.topic == "ehzmeter/pvtotal":
        e_total = float(msg)   # value is 0.1 Wh, divide by 10000 for kwh
    elif message.topic == "ehzmeter/pvpwrl123":
        str_l1, str_l2, str_l3 = msg.split(",")  # for comma-separated inputs
        pow_l1 = float(str_l1)
        pow_l2 = float(str_l2)
        pow_l3 = float(str_l3)
        print("powl123: ",pow_l1,pow_l2,pow_l3)

class DbusDummyService2:
  def __init__(self, servicename, deviceinstance, paths, productname='SMAL1 HM L1 L2 L3', connection='MQTT'):
    self._dbusservice = VeDbusService(servicename)
    self._paths = paths

    logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

    # Create the management objects, as specified in the ccgx dbus-api document
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Create the mandatory objects
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 0xFFFF) # value used in ac_sensor_bridge.cpp of dbus-cgwacs
    self._dbusservice.add_path('/ProductName', productname)
    self._dbusservice.add_path('/Position', 0)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    self._dbusservice.add_path('/HardwareVersion', 0)
    self._dbusservice.add_path('/Connected', 1)

    for path, settings in self._paths.items():
      self._dbusservice.add_path(
        path, settings['initial'], writeable=True, onchangecallback=self._handlechangedvalue)

    gobject.timeout_add(10000, self._update) # pause 10000ms before the next request

  # /Ac/Energy/Forward is in kwh, 73311 is the offset of the total energy at begin of 12.09.23
  def _update(self):
    e_totkwh = e_total/10000 - 73311
    self._dbusservice['/Ac/Energy/Forward'] = round((e_totkwh),1)
    self._dbusservice['/Ac/L1/Voltage'] = 230
    self._dbusservice['/Ac/L2/Voltage'] = 230
    self._dbusservice['/Ac/L3/Voltage'] = 230
    self._dbusservice['/Ac/L1/Energy/Forward'] = round((e_totkwh * 0.576),1)
    self._dbusservice['/Ac/L2/Energy/Forward'] = round((e_totkwh * 0.212),1)
    self._dbusservice['/Ac/L3/Energy/Forward'] = round((e_totkwh * 0.212),1)
    self._dbusservice['/Ac/L1/Current'] = round(pow_l1/230,1)
    self._dbusservice['/Ac/L2/Current'] = round(pow_l2/230,1)
    self._dbusservice['/Ac/L3/Current'] = round(pow_l3/230,1)
    self._dbusservice['/Ac/L1/Power'] = round(pow_l1,1)
    self._dbusservice['/Ac/L2/Power'] = round(pow_l2,1)
    self._dbusservice['/Ac/L3/Power'] = round(pow_l3,1)
    self._dbusservice['/Ac/Power'] = round(pow_ges)
    
    logging.info("PV Power: {:.0f}".format(pow_ges))
    logging.info("PV Energy: {:.4f}".format(e_totkwh))
    
    # increment UpdateIndex - to show that new data is available
    index = self._dbusservice[path_UpdateIndex] + 1  # increment index
    if index > 255:   # maximum value of the index
      index = 0       # overflow from 255 to 0
    self._dbusservice[path_UpdateIndex] = index
    return True

  def _handlechangedvalue(self, path, value):
    logging.debug("someone else updated %s to %s" % (path, value))
    return True # accept the change

def main():
  logging.basicConfig(level=logging.INFO) # use .INFO for less logging was .DEBUG
  thread.daemon = True # allow the program to quit

  from dbus.mainloop.glib import DBusGMainLoop
  # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
  DBusGMainLoop(set_as_default=True)
  
  pvac_output = DbusDummyService2(
    servicename='com.victronenergy.pvinverter.pv0',
    deviceinstance=41,
    paths={
      '/Ac/Power': {'initial': 0},
      '/Ac/L1/Voltage': {'initial': 0},
      '/Ac/L2/Voltage': {'initial': 0},
      '/Ac/L3/Voltage': {'initial': 0},
      '/Ac/L1/Current': {'initial': 0},
      '/Ac/L2/Current': {'initial': 0},
      '/Ac/L3/Current': {'initial': 0},
      '/Ac/L1/Power': {'initial': 0},
      '/Ac/L2/Power': {'initial': 0},
      '/Ac/L3/Power': {'initial': 0},
      '/Ac/Energy/Forward': {'initial': 0},
      '/Ac/L1/Energy/Forward': {'initial': 0},
      '/Ac/L2/Energy/Forward': {'initial': 0},
      '/Ac/L3/Energy/Forward': {'initial': 0},
      path_UpdateIndex: {'initial': 0},
    })

  logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
  mainloop = gobject.MainLoop()
  mainloop.run()

# Konfiguration MQTT
#client = mqtt.Client(MQTTNAME) # create new instance
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, MQTTNAME)
client.on_disconnect = on_disconnect
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker_address, port)  # connect to broker

client.loop_start()

if __name__ == "__main__":
  main()