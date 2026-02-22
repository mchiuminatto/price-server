import pytest
import io


@pytest.mark.anyio
async def test_upload_tick_data(client):
    create = await client.post(
        "/assets", json={"symbol": "EURUSD2", "name": "EUR/USD", "asset_type": "CURRENCY"}
    )
    instrument_id = create.json()["id"]

    csv_content = "timestamp,bid,ask,volume\n2024-01-01 00:00:00,1.10000,1.10002,100000\n2024-01-01 00:01:00,1.10001,1.10003,200000\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    data = {"instrument_id": str(instrument_id)}

    resp = await client.post("/prices/tick/upload", files=files, data=data)
    assert resp.status_code == 200
    assert resp.json()["inserted"] == 2


@pytest.mark.anyio
async def test_upload_ohlc_data(client):
    create = await client.post(
        "/assets", json={"symbol": "BTCUSD2", "name": "BTC/USD", "asset_type": "CRYPTO"}
    )
    instrument_id = create.json()["id"]

    csv_content = "timestamp,open,high,low,close,volume\n2024-01-01 00:00:00,42000,42500,41500,42200,1000\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    data = {"instrument_id": str(instrument_id), "timeframe": "H1", "price_type": "OHLC"}

    resp = await client.post("/prices/ohlc/upload", files=files, data=data)
    assert resp.status_code == 200
    assert resp.json()["inserted"] == 1


@pytest.mark.anyio
async def test_get_tick_data(client):
    create = await client.post(
        "/assets", json={"symbol": "GBPUSD", "name": "GBP/USD", "asset_type": "CURRENCY"}
    )
    instrument_id = create.json()["id"]

    csv_content = "timestamp,bid,ask,volume\n2024-01-01 00:00:00,1.27000,1.27002,100000\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    await client.post(
        "/prices/tick/upload", files=files, data={"instrument_id": str(instrument_id)}
    )

    resp = await client.get(f"/prices/tick/{instrument_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1


@pytest.mark.anyio
async def test_get_ohlc_data(client):
    create = await client.post(
        "/assets", json={"symbol": "ETHUSD", "name": "ETH/USD", "asset_type": "CRYPTO"}
    )
    instrument_id = create.json()["id"]

    csv_content = (
        "timestamp,open,high,low,close,volume\n2024-01-01 00:00:00,2200,2250,2150,2220,500\n"
    )
    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    await client.post(
        "/prices/ohlc/upload",
        files=files,
        data={"instrument_id": str(instrument_id), "timeframe": "D1", "price_type": "OHLC"},
    )

    resp = await client.get(f"/prices/ohlc/{instrument_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1
