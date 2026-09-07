import asyncio
import json
import requests
import websockets

from fastapi import FastAPI, WebSocket, WebSocketDisconnect


app = FastAPI()


# =====================================================
# MTAPI CONFIGURATION
# =====================================================

import os

MTAPI_URL = "https://mt5.mtapi.io"

LOGIN = os.getenv("MT5_LOGIN")
PASSWORD = os.getenv("MT5_PASSWORD")
SERVER = os.getenv("MT5_SERVER")


# =====================================================
# CONNECT TO MTAPI
# =====================================================

def connect_mtapi():

    response = requests.get(
        f"{MTAPI_URL}/ConnectEx",
        params={
            "user": LOGIN,
            "password": PASSWORD,
            "server": SERVER
        },
        timeout=30
    )

    if response.status_code != 200:

        raise Exception(
            f"ConnectEx failed: {response.text}"
        )

    session_id = response.text.strip().strip('"')

    return session_id


# =====================================================
# YOUR PUBLIC WEBSOCKET
# =====================================================

@app.websocket("/ws/tickdata")
async def tickdata(websocket: WebSocket):

    await websocket.accept()

    print("Client connected")

    try:

        # =================================================
        # WAIT FOR SYMBOL FROM CLIENT
        # =================================================

        request = await websocket.receive_json()

        symbol = request.get("symbol")

        if not symbol:

            await websocket.send_json({
                "type": "error",
                "message": "Symbol is required"
            })

            await websocket.close()

            return

        print(
            f"Client requested symbol: {symbol}"
        )

        # =================================================
        # CONNECT TO MTAPI
        # =================================================

        session_id = connect_mtapi()

        print(
            "MTAPI session:",
            session_id
        )

        # =================================================
        # SUBSCRIBE TO SYMBOL
        # =================================================

        subscribe_response = requests.get(
            f"{MTAPI_URL}/Subscribe",
            params={
                "id": session_id,
                "symbol": symbol
            },
            timeout=30
        )

        print(
            "Subscribe:",
            subscribe_response.status_code,
            subscribe_response.text
        )

        # =================================================
        # CONNECT TO MTAPI ONQUOTE
        # =================================================

        mtapi_ws_url = (
            f"wss://mt5.mtapi.io/OnQuote"
            f"?id={session_id}"
        )

        async with websockets.connect(
            mtapi_ws_url
        ) as mtapi_ws:

            print(
                f"Connected to MTAPI OnQuote: {symbol}"
            )

            # =================================================
            # RECEIVE MTAPI TICKS
            # =================================================

            while True:

                message = await mtapi_ws.recv()

                try:

                    data = json.loads(message)

                except json.JSONDecodeError:

                    continue

                # Only Quote events

                if data.get("type") != "Quote":

                    continue

                quote = data.get(
                    "data",
                    {}
                )

                quote_symbol = quote.get(
                    "symbol"
                )

                # Only requested symbol

                if quote_symbol != symbol:

                    continue

                # =================================================
                # SEND TO YOUR CLIENT
                # =================================================

                await websocket.send_json({

                    "type": "tick",

                    "symbol": quote_symbol,

                    "bid": quote.get("bid"),

                    "ask": quote.get("ask"),

                    "last": quote.get("last"),

                    "time": quote.get("time"),

                    "timestampUTC":
                        quote.get("timestampUTC")

                })

    except WebSocketDisconnect:

        print(
            "Client disconnected"
        )

    except Exception as e:

        print(
            "WebSocket error:",
            e
        )

        try:

            await websocket.send_json({

                "type": "error",

                "message": str(e)

            })

        except Exception:

            pass
@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "MT5 Live Market Data WebSocket",
        "websocket": "/ws/tickdata"
    }