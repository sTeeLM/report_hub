#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import configparser
import logging
import pidfile
import signal
import secrets
import importlib
from contextlib import asynccontextmanager
from typing import Dict, Any, Type, Tuple
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException, Request, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import uvicorn
import daemon

# Global configuration dictionary
CONFIG: Dict[str, Any] = {}

# ==============================================================================
# 1. Configuration and Arguments Parser with Short Form & Help Options
# ==============================================================================
def parse_arguments_and_config():
    # Enforce standard formatted help output using the native -h/--help mechanism
    parser = argparse.ArgumentParser(
        description="report data receiver service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Core Server Flags
    parser.add_argument("-c", "--config", type=str, default="/etc/report_hub/report_hub.conf", help="Configuration file path")
    parser.add_argument("-P", "--protocol", type=str, choices=["http", "https"], help="Service network protocol")
    parser.add_argument("-H", "--host", type=str, help="Service network listen bind address")
    parser.add_argument("-p", "--port", type=int, help="Service network listen bind port")
    parser.add_argument("-d", "--daemon", action="store_true", default=argparse.SUPPRESS, help="Run in POSIX background daemon mode")
    parser.add_argument("-f", "--log-file", type=str, help="Log storage target absolute file path")
    parser.add_argument("-l", "--log-level", type=str, choices=["debug", "info", "warning", "error"], help="Logging filter severity index")
    parser.add_argument("-s", "--service-dir", type=str, help="Service Modules")

    # PID File Flag with short form -m
    parser.add_argument("-m", "--pid-file", type=str, help="Path to save runtime process ID file (.pid)")

    # Native SSL/TLS Certificates Path Flags (Newly Added)
    parser.add_argument("-K", "--ssl-key", type=str, help="Path to target SSL private key file (.key)")
    parser.add_argument("-C", "--ssl-cert", type=str, help="Path to target SSL certificate bundle file (.crt/.pem)")

    # InfluxDB Pipeline Flags
    parser.add_argument("-U", "--influx-url", type=str, help="InfluxDB endpoint connection context URL")
    parser.add_argument("-T", "--influx-token", type=str, help="InfluxDB core security token key value")
    parser.add_argument("-O", "--influx-org", type=str, help="InfluxDB platform identity organization name")

    args = parser.parse_args()
    cmd_line_values = vars(args)

    defaults = {
        "protocol": "http", "host": "0.0.0.0", "port": 8989, "daemon": False,
        "log_file": "/var/log/report_hub/report_hub.log", "log_level": "info", "service_dir" : "/etc/report_hub/service.d",
        "pid_file": "/var/run/report_hub.pid",
        "ssl_key": "", "ssl_cert": "","influx_url": "http://127.0.0.1:8086", "influx_token": "",
        "influx_org": "my_org"
    }

    config_file_values = {}
    config_path = cmd_line_values.get("config", "/etc/report_hub/report_hub.conf")
    if os.path.exists(config_path):
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        if "server" in cfg:
            if "protocol" in cfg["server"]: config_file_values["protocol"] = cfg["server"].get("protocol")
            if "host" in cfg["server"]: config_file_values["host"] = cfg["server"].get("host")
            if "port" in cfg["server"]: config_file_values["port"] = cfg["server"].getint("port")
            if "daemon" in cfg["server"]: config_file_values["daemon"] = cfg["server"].getboolean("daemon")
            if "log_file" in cfg["server"]: config_file_values["log_file"] = cfg["server"].get("log_file")
            if "log_level" in cfg["server"]: config_file_values["log_level"] = cfg["server"].get("log_level")
            if "pid_file" in cfg["server"]: config_file_values["pid_file"] = cfg["server"].get("pid_file")
            if "service_dir" in cfg["server"]: config_file_values["service_dir"] = cfg["server"].get("service_dir")
            if "ssl_key" in cfg["server"]: config_file_values["ssl_key"] = cfg["server"].get("ssl_key")
            if "ssl_cert" in cfg["server"]: config_file_values["ssl_cert"] = cfg["server"].get("ssl_cert")
        if "influxdb" in cfg:
            if "url" in cfg["influxdb"]: config_file_values["influx_url"] = cfg["influxdb"].get("url")
            if "token" in cfg["influxdb"]: config_file_values["influx_token"] = cfg["influxdb"].get("token")
            if "org" in cfg["influxdb"]: config_file_values["influx_org"] = cfg["influxdb"].get("org")

    for key in defaults:
        if key in cmd_line_values and cmd_line_values[key] is not None:
            CONFIG[key] = cmd_line_values[key]
        elif key in config_file_values and config_file_values[key] is not None:
            CONFIG[key] = config_file_values[key]
        else:
            CONFIG[key] = defaults[key]

# ==============================================================================
# 2. Millisecond-level Logging & HUP Rotate System
# ==============================================================================
file_handler: logging.FileHandler = None
formatter: logging.Formatter = None

def setup_logging():
    global file_handler, formatter
    level_map = {
        "debug": logging.DEBUG, "info": logging.INFO,
        "warning": logging.WARNING, "error": logging.ERROR
    }
    log_level = level_map.get(CONFIG["log_level"].lower(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if CONFIG["daemon"]:
        log_dir = os.path.dirname(CONFIG["log_file"])
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception:
                pass
        file_handler = logging.FileHandler(CONFIG["log_file"])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

def handle_hup_signal(signum, frame):
    global file_handler, formatter
    if CONFIG["daemon"] and file_handler:
        logger = logging.getLogger()
        logger.info("SIGHUP received. Rotating log files...")
        logger.removeHandler(file_handler)
        file_handler.close()

        file_handler = logging.FileHandler(CONFIG["log_file"])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info("Log file has been successfully re-opened.")

def register_signal_handler():
    signal.signal(signal.SIGHUP, handle_hup_signal)


# ==============================================================================
# 3. Load Plugins
# ==============================================================================
PLUGIN_MODELS: Dict[str, Type[BaseModel]] = {}
# 凭证字典格式: {"服务名": ("用户名", "密码")}
PLUGIN_CREDENTIALS: Dict[str, Tuple[str, str]] = {}

def load_all_plugins_and_keys():
    """Scan directory and load python modules + security keys into memory"""
    PLUGIN_MODELS.clear()
    PLUGIN_CREDENTIALS.clear()

    SERVICE_DIR = CONFIG["service_dir"]

    if not os.path.exists(SERVICE_DIR):
        logging.error(f"Plugin directory not found: {SERVICE_DIR}")
        return

    for file_name in os.listdir(SERVICE_DIR):
        if file_name.endswith(".py") and not file_name.startswith("__"):
            service_name = file_name[:-3]
            script_path = os.path.join(SERVICE_DIR, file_name)
            key_path = os.path.join(SERVICE_DIR, f"{service_name}.key")

            # A. Dynamic Import via importlib (Safe & robust way)
            try:
                spec = importlib.util.spec_from_file_location(service_name, script_path)
                if spec is None or spec.loader is None:
                    logging.error(f"Could not load spec for {file_name}")
                    continue
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                if hasattr(module, "MODULE_MODEL"):
                    PLUGIN_MODELS[service_name] = getattr(module, "MODULE_MODEL")
                else:
                    logging.warning(f"[SKIP] Plugin {file_name} missing MODULE_MODEL definition")
                    continue
            except Exception as e:
                logging.error(f"[ERROR] Failed to import script {file_name}: {str(e)}")
                continue

            # B. Read corresponding .key file into the security mapping dictionary
            if os.path.exists(key_path):
                try:
                    with open(key_path, "r", encoding="utf-8") as f:
                        content = f.read().strip()
                        correct_username, correct_password = content.split(":", 1)
                        PLUGIN_CREDENTIALS[service_name] = (correct_username, correct_password)
                        logging.info(f"Successfully loaded plugin and credentials -> [{service_name}]")
                except Exception:
                    logging.warning(f"Invalid credentials format in: {service_name}.key")
                    PLUGIN_CREDENTIALS[service_name] = None
            else:
                logging.warning(f"Plugin [{service_name}] is missing an associated .key file")
                PLUGIN_CREDENTIALS[service_name] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize all resources at worker process startup
    load_all_plugins_and_keys()
    app.state.influx_client = InfluxDBClient(url=CONFIG["influx_url"], token=CONFIG["influx_token"], org=CONFIG["influx_org"])
    app.state.write_api = app.state.influx_client.write_api(write_options=SYNCHRONOUS)
    yield
    # Safely release resource pools when process shuts down
    logging.info("Closing influx client...")
    app.state.influx_client.close()

# ==============================================================================
# 4. FastAPI Web Service Core Application with HTTP Basic Auth Protection
# ==============================================================================
app = FastAPI(lifespan=lifespan)
security = HTTPBasic()

def verify_plugin_credentials(service: str, credentials: HTTPBasicCredentials):
    """Perform O(1) secure validation purely out of RAM allocation map"""
    correct_creds = PLUGIN_CREDENTIALS.get(service)

    if not correct_creds:
        logging.warning(f"None credential record for {service}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Unauthorized access or invalid service name: {service}",
            headers={"WWW-Authenticate": "Basic"}
        )

    correct_username, correct_password = correct_creds
    logging.info(f"Checking credential for {correct_username}")

    # Constant-time comparison to guard against remote timing analysis attacks
    is_username_correct = secrets.compare_digest(credentials.username, correct_username)
    is_password_correct = secrets.compare_digest(credentials.password, correct_password)

    if not (is_username_correct and is_password_correct):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed for the specified HTTP Basic profile",
            headers={"WWW-Authenticate": "Basic"},
        )

@app.post("/report", status_code=status.HTTP_201_CREATED)
async def receive_sensor_data(payload: Dict[str, Any], request: Request, service: str = Query(..., description="The targeting service router identifier"), credentials: HTTPBasicCredentials = Depends(security)):
    raw_body = await request.body()

    if not service:
        logging.error("No service send")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No service send")

    logging.info(f"Get payload from service {service} report: {payload}")

    # 1. Memory-isolated verification check
    verify_plugin_credentials(service, credentials)

    # 2. Extract corresponding Pydantic constructor
    model_cls = PLUGIN_MODELS.get(service)
    if not model_cls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Active schema engine not mapped for service: {service}"
        )

    try:
        # 3. Structural assertion and compilation payload conversion
        validated_data = model_cls.model_validate(payload)
        point_object = validated_data.to_influx_point()

        # 4. Stream transaction execution into InfluxDB Time Series cluster
        app.state.write_api.write(bucket=service, org=CONFIG["influx_org"], record=point_object)

        logging.info("Successfully parsed and committed payload to InfluxDB.")

        return {"status": "success", "service": service}

    except Exception as e:
        logging.error(f"Failed to process payload or write to InfluxDB: {str(e)}")
        if "ValidationError" in type(e).__name__:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal pipeline failure processing metrics: {str(e)}"
        )

def run_web_server():
    # 💡 Core Dynamic Logic: Inject SSL context attributes when protocol is explicitly set to https
    if CONFIG["protocol"].lower() == "https" and CONFIG["ssl_key"] and CONFIG["ssl_cert"]:
        logging.info("Enabling Native SSL/TLS Pipeline encryption for Uvicorn.")
        uvicorn.run(
            app,
            host=CONFIG["host"],
            port=CONFIG["port"],
            log_config=None,
            ssl_keyfile=CONFIG["ssl_key"],
            ssl_certfile=CONFIG["ssl_cert"]
        )
    else:
        if CONFIG["protocol"].lower() == "https":
            logging.warning("Protocol is set to https but SSL credentials are missing. Falling back to HTTP.")
        uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_config=None)

# ==============================================================================
# 5. Execution Entrance
# ==============================================================================
if __name__ == "__main__":
    parse_arguments_and_config()
    setup_logging()

    logging.info("Up report service")

    log_config = CONFIG.copy()
    if log_config.get("influx_token"):
        token = log_config["influx_token"]
        log_config["influx_token"] = f"{token[:6]}...{token[-6:]}" if len(token) > 12 else "******"

    logging.info("--- Core Configurations Multi-level Merging Results ---")
    logging.info(json.dumps(log_config, indent=4, ensure_ascii=False))
    logging.info("-------------------------------------------------------")

    if CONFIG["daemon"]:
        logging.info(f"Switching service to background Daemon mode. Log: {CONFIG['log_file']}")

        # Ensure the parent directory for the PID file exists before locking context
        pid_file_path = CONFIG["pid_file"]
        pid_dir = os.path.dirname(pid_file_path)
        if pid_dir and not os.path.exists(pid_dir):
            try:
                os.makedirs(pid_dir, exist_ok=True)
            except Exception:
                logging.error(f"Execution aborted. os.makedirs failed")
                pass
        context = daemon.DaemonContext(pidfile=pidfile.PidFile(pid_file_path))
        if file_handler and file_handler.stream:
            context.files_preserve = [file_handler.stream]
        with context:
            register_signal_handler()
            run_web_server()
    else:
        logging.info(f"Starting service in foreground. URL: {CONFIG['protocol']}://{CONFIG['host']}:{CONFIG['port']}")
        register_signal_handler()
        run_web_server()

