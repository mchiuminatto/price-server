import pytest


@pytest.mark.anyio
async def test_export_tick_request(client):
    create = await client.post(
        "/assets", json={"symbol": "USDJPY", "name": "USD/JPY", "asset_type": "CURRENCY"}
    )
    instrument_id = create.json()["id"]
    resp = await client.post(f"/prices/tick/{instrument_id}/export")
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data


@pytest.mark.anyio
async def test_export_ohlc_request(client):
    create = await client.post(
        "/assets", json={"symbol": "XAUUSD", "name": "Gold/USD", "asset_type": "COMMODITY"}
    )
    instrument_id = create.json()["id"]
    resp = await client.post(
        f"/prices/ohlc/{instrument_id}/export", params={"timeframe": "H1"}
    )
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data


@pytest.mark.anyio
async def test_export_status(client):
    create = await client.post(
        "/assets", json={"symbol": "USDCHF", "name": "USD/CHF", "asset_type": "CURRENCY"}
    )
    instrument_id = create.json()["id"]
    export = await client.post(f"/prices/tick/{instrument_id}/export")
    job_id = export.json()["job_id"]

    resp = await client.get(f"/prices/export/{job_id}/status")
    assert resp.status_code == 200
    assert "status" in resp.json()
