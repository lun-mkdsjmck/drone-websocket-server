import asyncio
import websockets

clients = {
    "controller_control": None,
    "drone_control": None,
    "controller_video": None,
    "drone_video": None
}

lock = asyncio.Lock()


async def handler(websocket):
    role = None

    try:
        # Первое сообщение от клиента = регистрация роли
        first = await websocket.recv()

        if isinstance(first, bytes):
            await websocket.close()
            return

        role = first.strip().lower()

        allowed_roles = {
            "controller_control",
            "drone_control",
            "controller_video",
            "drone_video"
        }

        if role not in allowed_roles:
            print(f"Unknown role: {role}")
            await websocket.close()
            return

        # Закрываем старое соединение той же роли
        async with lock:
            old_client = clients.get(role)

            if old_client is not None:
                try:
                    await old_client.close()
                except Exception:
                    pass

            clients[role] = websocket

        print(f"{role} connected")

        # Основной цикл
        while True:

            message = await websocket.recv()
            print(f"{role} -> {len(message) if isinstance(message, bytes) else message}")

            if role == "controller_control":
                target_role = "drone_control"

            elif role == "drone_control":
                target_role = "controller_control"

            elif role == "drone_video":
                target_role = "controller_video"

            elif role == "controller_video":
                target_role = "drone_video"

            else:
                continue

            async with lock:
                target = clients.get(target_role)

            if target is not None:
                try:
                    await target.send(message)

                except websockets.ConnectionClosed:
                    pass

                except Exception as e:
                    print(f"Send error: {e}")

    except websockets.ConnectionClosed:
        pass

    except Exception as e:
        print(f"{role}: {e}")

    finally:
        if role:
            async with lock:
                if clients.get(role) == websocket:
                    clients[role] = None

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
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
