import asyncio
import hashlib
import logging
from typing import Dict, Any, List

import httpx
from fastapi import FastAPI, Request, HTTPException
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------
# Config
# -----------------------------

ZABBIX_URL = "https://izabbix.telia.lt/api_jsonrpc.php"
ZABBIX_TOKEN = "4da659325065f6921f6553139ab9f2ca03d75b152fe6a05354814e9cf0222164"

WORKER_COUNT = 3
QUEUE_SIZE = 1000

# -----------------------------
# Logging
# -----------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alert_bridge")

# -----------------------------
# FastAPI
# -----------------------------

app = FastAPI()

alert_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
zabbix_token: str | None = None
zabbix_token_lock = asyncio.Lock()

dedup_cache: Dict[str, str] = {}

# -----------------------------
# Severity Mapping
# -----------------------------

SEVERITY_MAP = {
    "critical": 5,
    "warning": 3,
    "info": 2
}

# -----------------------------
# Zabbix API
# -----------------------------


async def zabbix_request(payload: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(ZABBIX_URL, json=payload)
        r.raise_for_status()
        return r.json()


async def zabbix_login():
    global zabbix_token

    payload = {
        "jsonrpc": "2.0",
        "method": "user.login",
        "params": {
            "sessionid": ZABBIX_TOKEN
        },
        "id": 1
    }

    result = await zabbix_request(payload)

    if "result" not in result:
        raise RuntimeError("Zabbix login failed")

    zabbix_token = result["result"]
    logger.info("Authenticated to Zabbix")


async def get_zabbix_token():

    global zabbix_token

    async with zabbix_token_lock:
        if not zabbix_token:
            await zabbix_login()

    return zabbix_token


# -----------------------------
# Retry wrapper
# -----------------------------

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def create_zabbix_event(name: str, message: str, severity: int):

    token = await get_zabbix_token()

    payload = {
        "jsonrpc": "2.0",
        "method": "event.create",
        "params": {
            "name": name,
            "severity": severity,
            "value": 1,
            "acknowledged": 0,
            "message": message
        },
        "auth": token,
        "id": 2
    }

    return await zabbix_request(payload)


# -----------------------------
# Deduplication
# -----------------------------

def alert_hash(labels: Dict[str, Any]) -> str:
    s = str(sorted(labels.items()))
    return hashlib.sha256(s.encode()).hexdigest()


# -----------------------------
# Worker
# -----------------------------

async def alert_worker():

    while True:
        alert = await alert_queue.get()

        try:
            await process_alert(alert)
        except Exception as e:
            logger.error(f"Failed processing alert: {e}")

        alert_queue.task_done()


# -----------------------------
# Alert Processing
# -----------------------------

async def process_alert(alert: Dict[str, Any]):

    status = alert.get("status")

    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})

    alertname = labels.get("alertname", "unknown")
    severity_label = labels.get("severity", "warning")

    summary = annotations.get("summary", "")
    description = annotations.get("description", "")

    severity = SEVERITY_MAP.get(severity_label, 3)

    message = f"""
        Alert: {alertname}
        Status: {status}

        Summary:
        {summary}

        Description:
        {description}

        Labels:
        {labels}
    """

    hash_id = alert_hash(labels)

    if status == "firing":

        if hash_id in dedup_cache:
            logger.info(f"Duplicate alert ignored: {alertname}")
            return

        dedup_cache[hash_id] = "active"

        logger.info(f"Creating Zabbix event: {alertname}")

        await create_zabbix_event(alertname, message, severity)

    elif status == "resolved":

        if hash_id in dedup_cache:
            del dedup_cache[hash_id]

        logger.info(f"Resolved alert: {alertname}")

        # Optional: send recovery message
        await create_zabbix_event(
            f"{alertname} resolved",
            message,
            1
        )


# -----------------------------
# Webhook Endpoint
# -----------------------------

@app.post("/alertmanager")
async def alertmanager_webhook(request: Request):

    payload = await request.json()

    if "alerts" not in payload:
        raise HTTPException(400, "Invalid payload")

    for alert in payload["alerts"]:
        await alert_queue.put(alert)

    return {"status": "queued", "alerts": len(payload["alerts"])}


# -----------------------------
# Startup
# -----------------------------

@app.on_event("startup")
async def startup():

    for _ in range(WORKER_COUNT):
        asyncio.create_task(alert_worker())

    logger.info("Alert workers started")
