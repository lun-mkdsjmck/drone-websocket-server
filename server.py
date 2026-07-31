import asyncio
import websockets
import socket

clients = {
    "controller_control": None,
    "drone_control": None,
    # WebSocket для видео нам больше не нужен, его заменяет UDP ниже
}

lock = asyncio.Lock()

# Global переменная для хранения текущего сотового IP-адреса пульта
pult_udp_address = None

# ==================== УЛЬТРАБЫСТРЫЙ UDP МОСТ ДЛЯ ВИДЕО ====================
def run_udp_video_repeater():
    global pult_udp_address
    
    # Открываем порты для видео и пинга
    video_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    video_socket.bind(('0.0.0.0', 5001))
    video_socket.setblocking(False)

    ping_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ping_socket.bind(('0.0.0.0', 5002))
    ping_socket.setblocking(False)

    print("📹 UDP Видео-мост запущен: Порт 5001 (Видео), Порт 5002 (Пинг Пульта)")

    receive_buffer = bytearray(1500)

    while True:
        # 1. Проверяем, пришел ли пинг от пульта, чтобы обновить его мобильный IP
        try:
            data, addr = ping_socket.recvfrom(1024)
            if data == b"PULSE":
                if pult_udp_address != (addr, 5001):
                    pult_udp_address = (addr, 5001)
                    print(f"🎮 UDP Пульт на связи! Маршрут видео зафиксирован на: {addr}")
        except BlockingIOError:
            pass

        # 2. Ловим пакет видео от дрона и швыряем его в пульт
        try:
            video_data, drone_addr = video_socket.recvfrom_into(receive_buffer)
            if video_data > 0 and pult_udp_address:
                video_socket.sendto(receive_buffer[:video_data], pult_udp_address)
        except BlockingIOError:
            pass
        except Exception as e:
            print(f"UDP Error: {e}")

# ==================== ВАШ СТАРЫЙ WEBSOCKET ДЛЯ КОМАНД ====================
async def handler(websocket):
    role = None
    try:
        first = await websocket.recv()
        if isinstance(first, bytes):
            await websocket.close()
            return

        role = first.strip().lower()
        allowed_roles = {"controller_control", "drone_control"}

        if role not in allowed_roles:
            print(f"Unknown role: {role}")
            await websocket.close()
            return

        async with lock:
            old_client = clients.get(role)
            if old_client:
                try: await old_client.close()
                except: pass
            clients[role] = websocket

        print(f"☁️ {role} connected via WebSocket")

        while True:
            message = await websocket.recv()
            if role == "controller_control":
                target_role = "drone_control"
            elif role == "drone_control":
                target_role = "controller_control"
            else:
                continue

            async with lock:
                target = clients.get(target_role)

            if target:
                try:
                    await target.send(message)
                except websockets.ConnectionClosed:
                    pass
                except Exception as e:
                    print(f"Send error: {e}")

    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        print(f"WS Exception [{role}]: {e}")
    finally:
        if role:
            async with lock:
                if clients.get(role) == websocket:
                    clients[role] = None
            print(f"☁️ {role} disconnected")


async def main():
    print("==================================================")
    print("🚀 ГИБРИДНЫЙ СЕРВЕР ДЛЯ ДРОНА ЗАПУЩЕН!")
    print("==================================================")
    
    # Запускаем UDP видео-поток в параллельном системном потоке, чтобы он не тормозил асинхронный сиквел
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, run_udp_video_repeater)

    # Запускаем ваш вебсокет для стиков управления на порту 8080
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
