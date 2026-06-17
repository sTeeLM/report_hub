from pydantic import BaseModel
from influxdb_client import Point
from influxdb_client.domain.write_precision import WritePrecision

class PMS5003STData(BaseModel):
    pm_10: int; pm_25: int; pm_100: int
    pm_10a: int; pm_25a: int; pm_100a: int
    pm_03cnt: int; pm_05cnt: int; pm_10cnt: int; pm_25cnt: int; pm_50cnt: int; pm_100cnt: int
    form: float; temp: float; mol: float

class ENS160Data(BaseModel):
    tvoc: float; eco2: float; iaq: int;

class BMP280Data(BaseModel):
    temp: float; press: float

class AHT20Data(BaseModel):
    temp: float; mol: float

class RS3231RTCData(BaseModel):
    temp:float;

class ESP32Data(BaseModel):
    temp: float;

class IVClockModel(BaseModel):
    pms5003st_data: PMS5003STData
    ens160_data: ENS160Data
    bmp280_data: BMP280Data
    aht20_data: AHT20Data
    ds3231_rtc_data: RS3231RTCData
    esp32_data: ESP32Data
    time_stamp: int
    device_id: str

    def to_influx_point(self) -> Point:
        """Explicitly build the InfluxDB Point using strong type fields"""
        point = Point("air_quality_data") \
            .tag("device_id", self.device_id) \
            .field("pms5003st_data_pm10", self.pms5003st_data.pm_10) \
            .field("pms5003st_data_pm25", self.pms5003st_data.pm_25) \
            .field("pms5003st_data_pm100", self.pms5003st_data.pm_100) \
            .field("pms5003st_data_pm10a", self.pms5003st_data.pm_10a) \
            .field("pms5003st_data_pm25a", self.pms5003st_data.pm_25a) \
            .field("pms5003st_data_pm100a", self.pms5003st_data.pm_100a) \
            .field("pms5003st_data_pm03cnt", self.pms5003st_data.pm_03cnt) \
            .field("pms5003st_data_pm05cnt", self.pms5003st_data.pm_05cnt) \
            .field("pms5003st_data_pm10cnt", self.pms5003st_data.pm_10cnt) \
            .field("pms5003st_data_pm25cnt", self.pms5003st_data.pm_25cnt) \
            .field("pms5003st_data_pm50cnt", self.pms5003st_data.pm_50cnt) \
            .field("pms5003st_data_pm100cnt", self.pms5003st_data.pm_100cnt) \
            .field("pms5003st_data_form", self.pms5003st_data.form) \
            .field("pms5003st_data_temp", self.pms5003st_data.temp) \
            .field("pms5003st_data_mol", self.pms5003st_data.mol) \
            .field("ens160_data_tvoc", self.ens160_data.tvoc) \
            .field("ens160_data_eco2", self.ens160_data.eco2) \
            .field("ens160_data_iaq", self.ens160_data.iaq) \
            .field("bmp280_data_temp", self.bmp280_data.temp) \
            .field("bmp280_data_press", self.bmp280_data.press) \
            .field("aht20_data_temp", self.aht20_data.temp) \
            .field("aht20_data_mol", self.aht20_data.mol) \
            .field("ds3231_rtc_data_temp", self.ds3231_rtc_data.temp) \
            .field("esp32_data_temp", self.esp32_data.temp) \
            .time(self.time_stamp, WritePrecision.S)
        return point

# Registration interface for the main application loader
MODULE_MODEL = IVClockModel
