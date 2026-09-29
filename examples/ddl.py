"""DROP TABLE through the GreptimeDB HTTP SQL API."""

import json
import urllib.error
import urllib.parse
import urllib.request


def execute_sql(sql, http_url="http://127.0.0.1:4000"):
    data = urllib.parse.urlencode({"sql": sql}).encode()
    request = urllib.request.Request(
        f"{http_url}/v1/sql?db=public",
        data=data,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read().decode()
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode()
        raise RuntimeError(payload) from exc
    body = json.loads(payload)
    if body.get("error"):
        raise RuntimeError(body["error"])
    return body


def drop_table(table):
    quoted = '"' + table.replace('"', '""') + '"'
    execute_sql(f"DROP TABLE IF EXISTS {quoted}")
