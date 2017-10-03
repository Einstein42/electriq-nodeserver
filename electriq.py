#!/usr/bin/python
""" Node Server for Polyglot 
      by Einstein.42(James Milne)
      milne.james@gmail.com"""

from __future__ import division      
from polyglot.nodeserver_api import SimpleNodeServer, PolyglotConnector, Node   
      
import sunspec.core.client as Sunspec
import time

PIKA_IP = '127.0.0.1'
PIKA_PORT = '19999'

VERSION = "1.0"
LOGGER = None

# Pika RCP States reported via device St
ST_UNKNOWN = 0
ST_DISABLED = 0x10
ST_INIT = 0x100
ST_POWERUP = 0x110
ST_CONNECTING_BUS = 0x120
ST_DISCONNECTING_BUS = 0x130
ST_TESTING = 0x140
ST_LOW_BUS_V = 0x200
ST_STANDBY = 0x300
ST_WAITING = 0x310
ST_CONNECTING_GRID = 0x800
ST_DISCONNECTING_GRID = 0x810
ST_CONNECTED = 0x820
ST_ISLANDED = 0x830
ST_LOW_INPUT_V = 0x1000
ST_CONNECTING_INPUT = 0x1100
ST_RUNNING = 0x2000
ST_MAKING_POWER = 0x2010
ST_LOW_SUN = 0x3100
ST_CHARGING = 0x6000
ST_DISCHARGING = 0x6100
ST_ERROR = 0x7000
ST_OVER_INPUT_V = 0x7010
ST_OVER_OUTPUT_V = 0x7020
ST_OVER_INPUT_A = 0x7030
ST_OVER_OUTPUT_A = 0x7040
ST_OVER_T = 0x7100
ST_GROUND_FAULT = 0x7200
RE_GENERIC = 0X8000
RE_POWERUP = 0X8010
RE_SUPPLY_UNDERVOLTAGE = 0X8020
RE_DAILY = 0x8030
RE_ENABLE = 0x8100
RE_DISABLE = 0x8200
RE_MANUAL_DISABLE = 0x8210
RE_EXTERNAL_ESTOP = 0x8220
RE_SYSMODE_CHANGE = 0x8300
RE_SYSMODE_BAD = 0x8400
RE_SOFTWARE_VERSION = 0x8FE0
RE_HARDWARE_VERSION = 0x8FF0
RE_INTERRUPT_FAULT = 0x9000
RE_GATEDRIVE_FAULT = 0x9100
RE_TIMER_FAULT = 0x9200
RE_TRANSISTOR_FAILURE = 0x9900
RE_OVER_TEMP = 0xA100
RE_BUS_FAULT = 0xB000
RE_BUS_NO_LOAD = 0xB010
RE_BUS_OVERVOLTAGE = 0xB100
RE_BUS_UNDERVOLTAGE = 0xB110
RE_BUS_OVERCURRENT = 0xB200
RE_INTERNAL_OVERVOLTAGE = 0xB300
RE_GROUND_FAULT = 0xC000
RE_INPUT_LOW = 0xC100
RE_INPUT_SENSOR_FAULT = 0xC200
RE_INPUT_OVER_CURRENT = 0xC300
RE_INPUT_OVERSPEED = 0xC400
RE_GRID_FAULT = 0xD000
RE_GRID_OVERVOLTAGE = 0xD010
RE_GRID_OVERCURRENT = 0xD020
RE_GRID_OVERFREQ = 0xD030
RE_GRID_UNDERVOLTAGE = 0xD040
RE_GRID_UNDERFREQ = 0xD050
RE_GRID_OVERVOLTAGE_FAST = 0xD060
RE_GRID_HIGH_IMPEDANCE = 0xD070

PIKASTATUS = {
    ST_ERROR:     "ERROR",
    ST_WAITING:   "WAITING",
    ST_CONNECTED: "CONNECTED", # battery is ready / operating
    ST_RUNNING:   "RUNNING", # inverter is ready
    ST_DISABLED:  "DISABLED",
    ST_UNKNOWN:   "UNKNOWN",
    ST_INIT:      "INITIALIZING",
    ST_POWERUP:   "POWERUP",
    ST_CONNECTING_BUS: "CONNECTING_BUS",
    ST_DISCONNECTING_BUS: "DISCONNECTING_BUS",
    ST_TESTING: "TESTING",
    ST_LOW_BUS_V: "LOW_BUS_V",
    ST_STANDBY: "STANDBY",
    ST_WAITING: "WAITING",
    ST_CONNECTING_GRID: "CONNECTING_GRID",
    ST_DISCONNECTING_GRID: "DISCONNECTING_GRID",
    ST_ISLANDED: "ISLANDED",
    ST_LOW_INPUT_V: "LOW_INPUT_V",
    ST_CONNECTING_INPUT: "CONNECTING_INPUT",
    ST_MAKING_POWER: "POWERING",
    ST_LOW_SUN: "LOW_SUN",
    ST_OVER_INPUT_V: "OVER_INPUT_V",
    ST_OVER_OUTPUT_V: "OVER_OUTPUT_V",
    ST_OVER_INPUT_A: "OVER_INPUT_A",
    ST_OVER_OUTPUT_A: "OVER_OUTPUT_A",
    ST_OVER_T: "OVER_T",
    ST_GROUND_FAULT: "GROUND_FAULT",
    ST_CHARGING: "CHARGING",
    ST_DISCHARGING: "DISCHARGING",
    RE_GENERIC: "RE_GENERIC",
    RE_POWERUP: "RE_POWERUP",
    RE_SUPPLY_UNDERVOLTAGE: "RE_SUPPLY_UNDERVOLTAGE",
    RE_DAILY: "RE_DAILY",
    RE_ENABLE: "RE_ENABLE",
    RE_DISABLE: "RE_DISABLE",
    RE_MANUAL_DISABLE: "RE_MANUAL_DISABLE",
    RE_EXTERNAL_ESTOP: "RE_EXTERNAL_ESTOP",
    RE_SYSMODE_CHANGE: "RE_SYSMODE_CHANGE",
    RE_SYSMODE_BAD: "RE_SYSMODE_BAD",
    RE_SOFTWARE_VERSION: "RE_SOFTWARE_VERSION",
    RE_HARDWARE_VERSION: "RE_HARDWARE_VERSION",
    RE_INTERRUPT_FAULT: "RE_INTERRUPT_FAULT",
    RE_GATEDRIVE_FAULT: "RE_GATEDRIVE_FAULT",
    RE_TIMER_FAULT: "RE_TIMER_FAULT",
    RE_TRANSISTOR_FAILURE: "RE_TRANSISTOR_FAILURE",
    RE_OVER_TEMP: "RE_OVER_TEMP",
    RE_BUS_FAULT: "RE_BUS_FAULT",
    RE_BUS_NO_LOAD: "RE_BUS_NO_LOAD",
    RE_BUS_OVERVOLTAGE: "RE_BUS_OVERVOLTAGE",
    RE_BUS_UNDERVOLTAGE: "RE_BUS_UNDERVOLTAGE",
    RE_BUS_OVERCURRENT: "RE_BUS_OVERCURRENT",
    RE_INTERNAL_OVERVOLTAGE: "RE_INTERNAL_OVERVOLTAGE",
    RE_GROUND_FAULT: "RE_GROUND_FAULT",
    RE_INPUT_LOW: "RE_INPUT_LOW",
    RE_INPUT_SENSOR_FAULT: "RE_INPUT_SENSOR_FAULT",
    RE_INPUT_OVER_CURRENT: "RE_INPUT_OVER_CURRENT",
    RE_INPUT_OVERSPEED: "RE_INPUT_OVERSPEED",
    RE_GRID_FAULT: "RE_GRID_FAULT",
    RE_GRID_OVERVOLTAGE: "RE_GRID_OVERVOLTAGE",
    RE_GRID_OVERCURRENT: "RE_GRID_OVERCURRENT",
    RE_GRID_OVERFREQ: "RE_GRID_OVERFREQ",
    RE_GRID_UNDERVOLTAGE: "RE_GRID_UNDERVOLTAGE",
    RE_GRID_UNDERFREQ: "RE_GRID_UNDERFREQ",
    RE_GRID_OVERVOLTAGE_FAST: "RE_GRID_OVERVOLTAGE_FAST",
    RE_GRID_HIGH_IMPEDANCE: "RE_GRID_HIGH_IMPEDANCE"
}

def myfloat(value, prec=2):
    """
    Round and return float
    
    :param value: Value to convert to float
    :param prec: Decimal places (default 2)
    """
    try:
        x = round(float(value), prec) 
    except (TypeError, ValueError):
        x = 0
    return x

class ElectrIQSystem(SimpleNodeServer):
    controller = None
    inverters = []
    batteries = []
    
    def setup(self):
        manifest = self.config.get('manifest',{})
        global LOGGER
        LOGGER = self.poly.logger
        LOGGER.info("FROM Poly ISYVER: %s", self.poly.isyver)        
        LOGGER.debug(manifest)
        self.controller = ElectrIQNode(self, manifest)
        self.controller.addDevices()
        self.update_config()
        if self.controller is not None:
            self.controller.query()
        if len(self.inverters) >= 1:
            for i in self.inverters:
                i.query()
        
    def poll(self):
        if self.controller is not None:
            self.controller.update_info()
        if len(self.inverters) >= 1:
            for i in self.inverters:
                i.update_info()
        self.update_config()


    def long_poll(self):
        pass

    def report_drivers(self):
        if self.controller is not None:
            self.controller.report_driver()
        if len(self.inverters) >= 1:
            for i in self.inverters:
                i.report_driver()

                
class ElectrIQNode(Node):
    """
    Instantiate the Main ElectrIQ Node.
    
    :param parent: Parent node device (ElectrIQSystem)
    :param primary: True/False if this is the primary node
    :param address: Address of the node for ISY
    :param manifest: Directory of config values
    
    .. autoattribute:: _drivers
    .. autoattribute:: _commands
    .. autoattribute:: node_def_id
    """
    
    def __init__(self, parent, manifest=None):
        self.parent = parent
        self.address = None
        self.name = 'ElectrIQ Pika Controller'
        self.serial = None
        self.pika = None 
        self.pika = openConnection(self.pika, 1)
        self.UpdtN = None
        self.Ct = None
        self.SysMd = None
        if readPoints(self.pika):
            LOGGER.info(common(self.pika))
            self.UpdtN = self.pika.REbus_dir.UpdtN
            self.Ct = self.pika.REbus_dir.Ct
            self.SysMd = self.pika.REbus_dir.SysMd
            self.address = 'pikacontrol'
            super(ElectrIQNode, self).__init__(parent, self.address, self.name, True, manifest)
            self.set_driver('GV1', self.UpdtN)
            self.set_driver('GV2', self.Ct)
            self.set_driver('GV3', self.SysMd)
        else:
            LOGGER.error('Failed to create control node, connection not established.')
            
    def addDevices(self):
        if self.pika is not None:
            for device in self.pika.REbus_dir.devices:
                if device != None:
                    if device.Dev == 7:
                        LOGGER.info('Inverter Found with Slave_ID of :{}'.format(device.UnitID))
                        self.parent.inverters.append(Inverter(self.parent, self, device))
                    elif device.Dev == 8:
                        LOGGER.info('Battery Controller Found with Slave_ID of :{}'.format(device.UnitID))
                        address = 'eq_btc_' + str(device.UnitID)
                        self.parent.inverters.append(Battery(self.parent, self, device))
            
    def update_info(self):
        """
        Update all the values (runs on long_poll)
        """
        if readPoints(self.pika):
            if self.pika:
                self.UpdtN = self.pika.REbus_dir.UpdtN
                self.Ct = self.pika.REbus_dir.Ct
                self.SysMd = self.pika.REbus_dir.SysMd
                self.set_driver('GV1', self.UpdtN)
                self.set_driver('GV2', self.Ct)
                self.set_driver('GV3', self.SysMd)
        else:
            LOGGER.error('Connection to Pika device was terminated, retrying.')
            self.pika = openConnection(self.pika, 1)
            if self.pika:
                self.update_info()
        return

    def query(self, **kwargs):
        """
        Get updated values for the registers
        """
        LOGGER.info('Query for all registers and report.')
        self.parent.long_poll()
        self.parent.report_drivers()
        # Wait a few seconds and do it again. The ISY has a bad habit of missing some updates
        time.sleep(10)
        self.parent.report_drivers()
        return True
        
    _drivers = {
                'GV1': [0, 56, int], 'GV2': [0, 56, int], 'GV3': [0, 56, int]
                }

    _commands = {'QUERY': query}
                            
    node_def_id = 'pika'

    
class Inverter(Node):
    """
    Instantiate an inverter node.
    
    :param parent: Parent node device (ElectrIQSystem)
    :param controller: Controller node device (ElectrIQNode)
    :param address: Address of the node for ISY
    :param device: The device value we discovered from the Pika
    :param manifest: Directory of config values
    
    .. autoattribute:: _drivers
    .. autoattribute:: _commands
    .. autoattribute:: node_def_id
    """
    def __init__(self, parent, controller, device):
        self.parent = parent
        self.controller = controller
        self.device = device
        self.slaveId = self.device.UnitID
        self.inv = None
        self.readInterval = 5
        self.VArPrevious = 0
        self.cumVArhPrevious = 0
        self.cumVArh = 0
        self.cumkVArh = 0
        LOGGER.info('Created Inverter, attempting connction to inverter at unit id: {}'.format(self.slaveId))
        self.inv = openConnection(self.inv, self.slaveId)
        self.address = 'eq_inv_' + str(self.slaveId)
        self.name = 'Inverter ID ' + str(self.slaveId)
        readPoints(self.inv)
        if self.inv:
            super(Inverter, self).__init__(parent, self.address, self.name, self.controller, None)
            try:
                self.VArPrevious = self.manifest['drivers']['ST']
            except KeyError:
                pass
            try:
                self.cumVArhPrevious = self.manifest['drivers']['RR']
            except KeyError:
                pass                
            self.VAr = self.VArPrevious
            if self.inv.common.Mn != None:
                LOGGER.info(common(self.inv))

    def update_info(self):
        if readPoints(self.controller.pika) and readPoints(self.inv):
            for device in self.controller.pika.REbus_dir.devices:
                if device != None:
                    if device.Dev == 7:
                        self.device = device
            if self.device:
                self.St = self.device.St
                self.Ena = self.device.Ena
                self.P = self.device.P # Watts
                self.O2 = fixsign(self.device.O2)
            if self.inv:
                self.A = self.inv.inverter.A
                self.V = self.inv.inverter.PhVphA
                self.W = self.inv.inverter.W
                self.WH = round(self.inv.inverter.WH / 1000, 2)
                self.Hz = self.inv.inverter.Hz
                self.VA = self.inv.inverter.VA
                self.VArPrevious = self.VAr
                self.VAr = self.inv.inverter.VAr
                self.cumVArhPrevious = self.cumVArh
                try:
                    self.cumVArh = self.cumVArhPrevious + (self.readInterval/3600) * (self.VArPrevious + self.VAr)
                except ZeroDivisionError:
                    self.cumVArh = 0
                try:
                    self.cumkVArh = self.cumVArh / 1000
                except ZeroDivisonError:
                    self.cumkVArh = 0
                self.PF = self.inv.inverter.PF
                self.DCV = self.inv.inverter.DCV
                self.PMaxLimPct = self.inv.REbus_arb.PMaxLimPct
                self.QMaxLimPct = self.inv.REbus_arb.QMaxLimPct
                self.update_drivers()
        else:
            LOGGER.error('Connection to Inverter device was terminated, retrying.')
            self.inv = openConnection(self.inv, self.slaveId)
            if self.inv:
                time.sleep(1)
                self.update_info()
        return
        
    def update_drivers(self):
        self.set_driver('GV1', self.St)
        self.set_driver('GV2', self.Ena)
        self.set_driver('GV3', self.P)
        self.set_driver('GV4', self.O2)
        self.set_driver('GV5', self.A)
        self.set_driver('GV6', self.V)
        self.set_driver('GV7', self.W)
        self.set_driver('GV8', self.WH)
        self.set_driver('GV9', self.Hz)
        self.set_driver('GV10', self.VA)
        self.set_driver('GV11', self.VAr)
        self.set_driver('GV12', self.PF)
        self.set_driver('GV13', self.DCV)
        self.set_driver('GV14', self.PMaxLimPct)
        self.set_driver('GV15', self.QMaxLimPct)
        self.set_driver('ST', self.cumVArh)
        self.set_driver('RR', self.cumkVArh)
        

    def query(self, **kwargs):
        """
        Get updated values for the registers
        """
        self.update_info()
        self.report_driver()
        return True
        
    _drivers = {
                'GV1': [0, 56, int], 'GV2': [0, 2, myfloat], 'GV3': [0, 73, myfloat],
                'GV4': [0, 73, myfloat], 'GV5': [0, 1, myfloat], 'GV6': [0, 72, myfloat],
                'GV7': [0, 73, myfloat], 'GV8': [0, 33, myfloat], 'GV9': [0, 90, myfloat],
                'GV10': [0, 56, myfloat], 'GV11': [0, 56, myfloat], 'GV12': [0, 51, myfloat],
                'GV13': [0, 72, myfloat], 'GV14': [0, 51, myfloat], 'GV15': [0, 51, myfloat],
                'ST': [0, 56, myfloat], 'RR': [0, 56, myfloat]
                }

    _commands = {'QUERY': query}

    node_def_id = 'inverter'    

    
class Battery(Node):
    """
    Instantiate a Battery node.
    
    :param parent: Parent node device (ElectrIQSystem)
    :param controller: Controller node device (ElectrIQNode)
    :param address: Address of the node for ISY
    :param device: The device value we discovered from the Pika
    :param manifest: Directory of config values
    
    .. autoattribute:: _drivers
    .. autoattribute:: _commands
    .. autoattribute:: node_def_id
    """
    def __init__(self, parent, controller, device):
        self.parent = parent
        self.controller = controller
        self.device = device
        self.slaveId = self.device.UnitID
        self.bat = None
        LOGGER.info('Created Battery Controller, attempting connction to controller at unit id: {}'.format(self.slaveId))
        self.bat = openConnection(self.bat, self.slaveId)
        self.address = 'eq_bat_' + str(self.slaveId)
        self.name = 'Battery Controller ID ' + str(self.slaveId)
        if self.bat:
            readPoints(self.bat)
            super(Battery, self).__init__(parent, self.address, self.name, self.controller, None)
            if self.bat.common.Mn != None:
                LOGGER.info(common(self.bat))        

    def update_info(self):
        if readPoints(self.controller.pika) and readPoints(self.bat):
            for device in self.controller.pika.REbus_dir.devices:
                if device != None:
                    if device.Dev == 8:
                        self.device = device
            if self.device:
                self.St = self.device.St
                #LOGGER.debug(hexstatus(self.St))
                self.Ena = self.device.Ena
                self.P = self.device.P # Watts
                self.T = self.device.T
                #self.E = self.device.E / 1000 # Watthours
                #self.I = self.device.I
                #self.O1 = self.device.O1
                #self.O5 = self.device.O5
            if self.bat:
                self.SoCMin = self.bat.battery.SoCMin
                self.SoCMax = self.bat.battery.SoCMax
                self.SoCRsvMin = self.bat.battery.SoCRsvMin
                self.SocRsvMax = self.bat.battery.SocRsvMax
                self.A = self.bat.battery.A
                self.V = self.bat.battery.V
                self.W = self.bat.battery.W
                self.CellVMin = self.bat.battery.CellVMin
                self.CellVMax = self.bat.battery.CellVMax
                self.SoC = self.bat.battery.SoC
                self.SoH = self.bat.battery.SoH
                self.update_drivers()
        else:
            LOGGER.error('Connection to Battery Controller device was terminated, retrying.')
            self.bat = openConnection(self.bat, self.slaveId)
            if self.bat:
                time.sleep(1)
                self.update_info()        
        return
        
    def update_drivers(self):
        self.set_driver('GV1', self.St)
        self.set_driver('GV2', self.Ena)
        self.set_driver('GV3', self.P)
        self.set_driver('GV4', self.T)
        self.set_driver('GV5', self.SoCMin)
        self.set_driver('GV6', self.SoCMax)
        self.set_driver('GV7', self.SoCRsvMin)
        self.set_driver('GV8', self.SocRsvMax)
        self.set_driver('GV9', self.A)
        self.set_driver('GV10', self.V)
        self.set_driver('GV11', self.W)
        self.set_driver('GV12', self.CellVMin)
        self.set_driver('GV13', self.CellVMax)
        self.set_driver('GV14', self.SoC)
        self.set_driver('GV15', self.SoH)

    def query(self, **kwargs):
        """
        Get updated values for the registers
        """
        self.update_info()
        self.report_driver()
        return True
        
    _drivers = {
                'GV1': [0, 56, myfloat], 'GV2': [0, 2, int], 'GV3': [0, 73, myfloat],
                'GV4': [0, 4, myfloat], 'GV5': [0, 51, myfloat], 'GV6': [0, 51, myfloat],
                'GV7': [0, 51, myfloat], 'GV8': [0, 51, myfloat], 'GV9': [0, 1, myfloat],
                'GV10': [0, 72, myfloat], 'GV11': [0, 73, myfloat], 'GV12': [0, 72, myfloat],
                'GV13': [0, 72, myfloat], 'GV14': [0, 51, myfloat], 'GV15': [0, 51, myfloat]
                }

    _commands = {'QUERY': query}

    node_def_id = 'battery'    
    
    
def convertCT(o2):
    if o2 > 30000:
        ct_power = -(2**16 - o2)
    else:
        ct_power == o2
    return ct_power

def common(model):
    if model:
        c = model.common
        return 'Found device: Mn: {} Md: {} Vr: {} SN: {}'.format(c.Mn, c.Md, c.Vr, c.SN)
    
def readPoints(model):
    if not model: return False
    try:
        model.read()
        return True
    except (Exception) as ex:
        LOGGER.error('readPoints Exception: {}'.format(ex))
        return False
    
def openConnection(device, slaveId):
    """
    The openConnection method to open/re-open connection to the Pika
    """
    if device: device.close()
    try:
        d = Sunspec.SunSpecClientDevice(Sunspec.TCP, slaveId, ipaddr=PIKA_IP, ipport=int(PIKA_PORT))
        LOGGER.info('Modbus connection successful for slave ID: {}'.format(str(slaveId)))
        return d
    except (Exception) as ex:
        LOGGER.error('connection error {}'.format(ex))
        return None        

def fixsign (uint) :
    # a 16 bit unsigned int is recast to be signed
    # fix the pika O2 register value is signed
    return uint if uint < 32768 else uint - 65536        
        
def hexstatus(d) :
    st = d if isinstance(d, int) else \
         d.St
    masked = st & 0xFFF0
    if masked in PIKASTATUS :
        status = PIKASTATUS[masked]
        status += "("+hex(st)+")" if st&0x000F != 0 else ""
        return  status
    else :
        return hex(st)

def main():
    """Setup connection, node server, and nodes"""
    poly = PolyglotConnector()
    # Override shortpoll and longpoll timers to 5/30, once per second is excessive in this nodeserver 
    nserver = ElectrIQSystem(poly, 5, 30)
    poly.connect()
    poly.wait_for_config()
    poly.logger.info("ElectrIQ Interface version " + VERSION + " created. Initiating setup.")
    nserver.setup()
    poly.logger.info("Setup completed. Running Server.")
    nserver.run()
    
if __name__ == "__main__":
    main()