#!/usr/bin/python
""" Node Server for Polyglot 
      by Einstein.42(James Milne)
      milne.james@gmail.com"""

import sunspec.core.client as Sunspec
import sunspec.core.modbus as Modbus

try:
    d = Sunspec.SunSpecClientDevice(Sunspec.TCP, 1, ipaddr='127.0.0.1', ipport=19999)
except Exception as ex:
    print('connection error {}'.format(ex))

d.device.read_points()
d.close()
print d
lcm = d.REbus_dir
for device in lcm.devices:
    if device != None:
        try:
            inv = Sunspec.SunSpecClientDevice(Sunspec.TCP, device.UnitID, ipaddr='127.0.0.1', ipport=19999)
        except (Sunspec.SunSpecClientError, Modbus.ModbusClientError) as ex:
            print('connection error {}'.format(ex))
        inv.device.read_points()
        inv.close()
        print inv
        

