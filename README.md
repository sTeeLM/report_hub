## 插件化数据上报服务 / Plugin-Based Data Reporter Service
这是一个基于 FastAPI 和 Pydantic v2 构建的轻量级物联网（IoT）数据上报服务。系统接收多层嵌套的传感器 JSON 遥测数据，通过内存中的 Pydantic 插件模型进行高速校验，并将其显式组装写入 InfluxDB v2。
This is a lightweight IoT metrics ingestion pipeline built on FastAPI and Pydantic v2. It receives sensor JSON telemetry payloads via HTTP POST, validates them using in-memory Pydantic plugins, and streams the structured records directly into InfluxDB v2.
------------------------------
## 📂 项目结构 / Project Blueprint

report_hub/
├── report_hub.py          # FastAPI 核心主程序 (FastAPI core service)
└── service.d/             # 插件沙盒目录 (Plugin rules sandboxed directory)
    ├── ivclock.py         # 强类型数据解析脚本 (Strong-type validation parser)
    └── ivclock.key        # 独立的 Basic Auth 凭证 (Independent credential: user:pass)

------------------------------
## ⚡ 快速使用 / Quick Start## 1. 安装依赖 / Install Dependencies

pip install fastapi uvicorn pydantic influxdb-client

## 2. 编写凭证与插件 / Create Credentials & Plugin

* service_rules/ivclock.key：写入一行标准凭证（格式为 用户名:密码）：

admin:my_secret_password_123

* service_rules/ivclock.py：定义数据结构，并将主模型绑定到 MODULE_MODEL：

from datetime import datetime, timezonefrom pydantic import BaseModelfrom influxdb_client import Point
class PMS5003STData(BaseModel):
    pm_10: float
    pm_25: float
class IvClockModel(BaseModel):
    device_id: str
    time_stamp: int
    pms5003st_data: PMS5003STData

    def to_influx_point(self) -> Point:
        dt = datetime.fromtimestamp(self.time_stamp, tz=timezone.utc)
        return (
            Point("air_quality_data")
            .tag("device_id", self.device_id)
            .time(dt)
            .field("pms5003st_data_pm10", self.pms5003st_data.pm_10)
            .field("pms5003st_data_pm25", self.pms5003st_data.pm_25)
        )
MODULE_MODEL = IVClockModel


## 3. 运行服务 / Launch Server
配置好 report_hub.py 顶部的 InfluxDB 连接参数后，执行：

python report_hub.py

## 4. 接口调用 / Test Endpoint

curl -X POST "http://127.0.0" \
     -u "admin:my_secret_password_123" \
     -H "Content-Type: application/json" \
     -d '{"device_id":"ivclock01","time_stamp":1781691060,"pms5003st_data":{"pm_10":11,"pm_25":17}}'

------------------------------
## ⚙️ 动态扩展 / Extension
新增设备时，只需在 service.d/ 目录下放置同名的 .py 脚本和 .key 密钥文件。服务无需重启，内存字典会自动热加载并生效。
To add a new device, simply drop the corresponding .py and .key files into service_rules/. The service will instantly hot-reload the definitions into memory without requiring a restart.



