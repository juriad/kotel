from influxdb import InfluxDBClient


class InfluxLoader:
    def __init__(self, hostname, port, username, password, database, prefix, logger):
        self.client = InfluxDBClient(hostname, port, username, password)
        self.client.switch_database(database)
        logger.debug('Connected to influx at %s with username %s, using database %s',
                     hostname, username, database)
        self.prefix = prefix
        self.logger = logger

        def _a(p, i):
            return lambda data: data[p][i]

        def _n(p, i):
            return lambda data: 1 - data[p][i]

        def _season(data):
            sm = data['h']['__R190_USINT_u']
            sv = data['h']['__R196_USINT_u']
            return (1 if sv == 0 else 0) if sm == 0 else 2

        self.measurements = {
            'heating': {
                'manual_regulation_point': _a('h', '__R2373.1_BOOL_i'),
                'manual_regulation_point_temperature': _a('h', '__R2376_REAL_.1f'),
                'curve_number': _a('h', '__R2369_USINT_d'),
                'curve_shift_comfort': _a('h', '__R2502_REAL_.1f'),
                'curve_shift_attenuation': _a('h', '__R2516_REAL_.1f'),
                'prewarming': _a('h', '__R2362.1_BOOL_i'),
                'season': _season,
                'desired': _a('h', '__R23596_REAL_.1f'),
                'backwater': _a('t', '__R23101_REAL_.1f'),
                'status': _a('s', '__R24261.0_BOOL_i')
            },
            'hot_water': {
                'enabled': _a('w', '__R4501.1_BOOL_i'),
                'comfort': _a('w', '__R4513_REAL_.1f'),
                'attenuation': _a('w', '__R4541_REAL_.1f'),
                'desired': _a('w', '__R23612_REAL_.1f'),
                'temperature': _a('t', '__R23107_REAL_.1f'),
                'status': _a('s', '__R24435.0_BOOL_i')
            },
            'compressor': {
                'enabled': _n('c', '__R811.1_BOOL_i'),
                'total_time': _a('c', '__R23658_UDINT_u'),
                'temperature': _a('t', '__R23083_REAL_.1f'),
                'status': _a('s', '__R24434.6_BOOL_i')
            },
            'boiler': {
                'enabled': _n('b', '__R1747.1_BOOL_i'),
                'threshold': _a('b', '__R1858_REAL_.1f'),
                'status_1': _a('s', '__R24029.0_BOOL_i'),
                'status_2': _a('s', '__R24056.0_BOOL_i'),
                'status_3': _a('s', '__R24083.0_BOOL_i')
            },
            'evaporator': {
                'outdoors': _a('t', '__R23065_REAL_.1f'),
                'evaporator': _a('t', '__R23071_REAL_.1f'),
                'status_fan': _a('s', '__R24137.0_BOOL_i')
            },
            'heat_pump': {
                'input': _a('t', '__R23053_REAL_.1f'),
                'output': _a('t', '__R23059_REAL_.1f'),
                'status': _a('s', '__R24434.7_BOOL_i')
            }
        }

    def _apply(self, data, measurement):
        return {
            'measurement': self.prefix + measurement,
            'fields': {
                key: desc(data)
                for (key, desc) in self.measurements[measurement].items()
            }
        }

    def load(self, data, measurements=None):
        if measurements is None:
            measurements = self.measurements.keys()
        stored = 0
        for measurement in measurements:
            try:
                it = self._apply(data, measurement)
            except KeyError as e:
                self.logger.error('Incorrect input data; missing key; data=%s', data, exc_info=e)
                raise e
            self.logger.debug('Storing data point of measurement %s into influx', self.prefix + measurement)
            self.logger.debug('Fields: %s', it)
            stored += 1 if self.client.write_points([it], 's') else 0
        self.logger.info('%d out of %d measurements have been stored in influx database', stored, len(measurements))
