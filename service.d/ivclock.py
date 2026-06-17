from pydantic import BaseModel
from influxdb_client import Point

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

class MultiSensorPayload(BaseModel):
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
            .tag("device_id", payload.device_id) \
            .field("pms5003st_data_pm10", payload.pms5003st_data.pm_10) \
            .field("pms5003st_data_pm25", payload.pms5003st_data.pm_25) \
            .field("pms5003st_data_pm100", payload.pms5003st_data.pm_100) \
            .field("pms5003st_data_pm10a", payload.pms5003st_data.pm_10a) \
            .field("pms5003st_data_pm25a", payload.pms5003st_data.pm_25a) \
            .field("pms5003st_data_pm100a", payload.pms5003st_data.pm_100a) \
            .field("pms5003st_data_pm03cnt", payload.pms5003st_data.pm_03cnt) \
            .field("pms5003st_data_pm05cnt", payload.pms5003st_data.pm_05cnt) \
            .field("pms5003st_data_pm10cnt", payload.pms5003st_data.pm_10cnt) \
            .field("pms5003st_data_pm25cnt", payload.pms5003st_data.pm_25cnt) \
            .field("pms5003st_data_pm50cnt", payload.pms5003st_data.pm_50cnt) \
            .field("pms5003st_data_pm100cnt", payload.pms5003st_data.pm_100cnt) \
            .field("pms5003st_data_form", payload.pms5003st_data.form) \
            .field("pms5003st_data_temp", payload.pms5003st_data.temp) \
            .field("pms5003st_data_mol", payload.pms5003st_data.mol) \
            .field("ens160_data_tvoc", payload.ens160_data.tvoc) \
            .field("ens160_data_eco2", payload.ens160_data.eco2) \
            .field("ens160_data_iaq", payload.ens160_data.iaq) \
            .field("bmp280_data_temp", payload.bmp280_data.temp) \
            .field("bmp280_data_press", payload.bmp280_data.press) \
            .field("aht20_data_temp", payload.aht20_data.temp) \
            .field("aht20_data_mol", payload.aht20_data.mol) \
            .field("ds3231_rtc_data_temp", payload.ds3231_rtc_data.temp) \
            .field("esp32_data_temp", payload.esp32_data.temp) \
            .time(payload.time_stamp, WritePrecision.S)
        return point

# Registration interface for the main application loader
MODULE_MODEL = IvClockModel
