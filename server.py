import asyncio
import websockets

clients = {}
lock = asyncio.Lock()

async def handler(websocket):
    role = None

    try:
        first = await websocket.recv()

        if isinstance(first, bytes):
            await websocket.close()
            return

        role = first.strip().lower()

        if role not in ["drone", "controller"]:
            await websocket.send("Send first message: drone or controller")
            await websocket.close()
            return

        async with lock:
            clients[role] = websocket

        print(f"{role} connected")

        while True:
            message = await websocket.recv()

            target = None

            async with lock:
                if role == "drone":
                    target = clients.get("controller")
                else:
                    target = clients.get("drone")

            if target:
                try:
                    await target.send(message)
                except:
                    pass
                   except websockets.ConnectionClosed:
        pass

    except Exception as e:
        print(e)

    finally:
        if role:
            async with lock:
                if clients.get(role) == websocket:
                    del clients[role]

        print(f"{role} disconnected")


async def main():
    print("WebSocket server started")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        8080,
        max_size=None,
        max_queue=None,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
  
