import pytest


@pytest.mark.anyio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_create_asset(client):
    resp = await client.post(
        "/assets",
        json={"symbol": "EURUSD", "name": "Euro/US Dollar", "asset_type": "CURRENCY"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["symbol"] == "EURUSD"


@pytest.mark.anyio
async def test_list_assets(client):
    await client.post(
        "/assets", json={"symbol": "BTCUSD", "name": "Bitcoin", "asset_type": "CRYPTO"}
    )
    resp = await client.get("/assets")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data


@pytest.mark.anyio
async def test_get_asset(client):
    create = await client.post(
        "/assets", json={"symbol": "GOLD", "name": "Gold", "asset_type": "COMMODITY"}
    )
    asset_id = create.json()["id"]
    resp = await client.get(f"/assets/{asset_id}")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_get_asset_not_found(client):
    resp = await client.get("/assets/9999")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_update_asset(client):
    create = await client.post(
        "/assets", json={"symbol": "AAPL", "name": "Apple", "asset_type": "EQUITY"}
    )
    asset_id = create.json()["id"]
    resp = await client.put(f"/assets/{asset_id}", json={"name": "Apple Inc."})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Apple Inc."


@pytest.mark.anyio
async def test_delete_asset(client):
    create = await client.post(
        "/assets", json={"symbol": "TSLA", "name": "Tesla", "asset_type": "EQUITY"}
    )
    asset_id = create.json()["id"]
    resp = await client.delete(f"/assets/{asset_id}")
    assert resp.status_code == 204
