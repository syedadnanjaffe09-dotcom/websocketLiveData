import os
import json
import asyncio
import websockets

from fastapi import FastAPI, WebSocket, WebSocketDisconnect


app = FastAPI()


# =====================================================
# TWELVE DATA CONFIGURATION
# =====================================================

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

TWELVE_DATA_WS_URL = (
    "wss://ws.twelvedata.com/v1/quotes/price"
)


# =====================================================
# HEALTH CHECK
# =====================================================

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Twelve Data Live Market Data WebSocket",
        "websocket": "/ws/tickdata"
    }


# =====================================================
# PUBLIC WEBSOCKET
# =====================================================

@app.websocket("/ws/tickdata")
async def tickdata(websocket: WebSocket):

    await websocket.accept()

    print("Client connected")

    twelve_ws = None

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
        # CHECK API KEY
        # =================================================

        if not TWELVE_DATA_API_KEY:

            await websocket.send_json({
                "type": "error",
                "message": "TWELVE_DATA_API_KEY is not configured"
            })

            await websocket.close()

            return

        # =================================================
        # TWELVE DATA WEBSOCKET
        # =================================================

        twelve_ws_url = (
            f"{TWELVE_DATA_WS_URL}"
            f"?apikey={TWELVE_DATA_API_KEY}"
        )

        print("Connecting to Twelve Data...")

        twelve_ws = await websockets.connect(
            twelve_ws_url,
            ping_interval=20,
            ping_timeout=20
        )

        print("Connected to Twelve Data")

        # =================================================
        # SUBSCRIBE TO SYMBOL
        # =================================================

        subscribe_message = {
            "action": "subscribe",
            "params": {
                "symbols": symbol
            }
        }

        await twelve_ws.send(
            json.dumps(subscribe_message)
        )

        print(
            f"Subscribed to Twelve Data symbol: {symbol}"
        )

        # =================================================
        # RECEIVE TWELVE DATA EVENTS
        # =================================================

        while True:

            message = await twelve_ws.recv()

            try:

                data = json.loads(message)

            except json.JSONDecodeError:

                print(
                    "Invalid JSON from Twelve Data:",
                    message
                )

                continue

            print(
                "Twelve Data:",
                data
            )

            # =================================================
            # PRICE EVENT
            # =================================================

            if data.get("event") == "price":

                quote_symbol = data.get("symbol")

                price = data.get("price")

                timestamp = data.get("timestamp")

                # Only requested symbol

                if quote_symbol != symbol:

                    continue

                # =================================================
                # SEND TO YOUR CLIENT
                # =================================================

                await websocket.send_json({

                    "type": "tick",

                    "symbol": quote_symbol,

                    "price": price,

                    "bid": None,

                    "ask": None,

                    "last": price,

                    "timestamp": timestamp,

                    "source": "twelvedata"

                })

            # =================================================
            # SUBSCRIBE STATUS
            # =================================================

            elif data.get("event") == "subscribe-status":

                await websocket.send_json({

                    "type": "subscription",

                    "data": data

                })

            # =================================================
            # ERROR
            # =================================================

            elif data.get("event") == "error":

                await websocket.send_json({

                    "type": "error",

                    "message": data

                })

    except WebSocketDisconnect:

        print(
            "Client disconnected"
        )

    except Exception as e:

        print(
            "WebSocket error:",
            repr(e)
        )

        try:

            await websocket.send_json({

                "type": "error",

                "message": str(e)

            })

        except Exception:

            pass

    finally:

        # =================================================
        # CLOSE TWELVE DATA CONNECTION
        # =================================================

        if twelve_ws:

            try:

                await twelve_ws.close()

            except Exception:

                pass

        print(
            "Twelve Data connection closed"
        )