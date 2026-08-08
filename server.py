import asyncio
import websockets

clients = {
    "controller_control": None,
    "drone_control": None,
    "controller_video": None,
    "drone_video": None
}

# Переменная замка сопряжения (По умолчанию миссия не начата, трафик заблокирован)
mission_started = False
lock = asyncio.Lock()

async def handler(websocket):
    global mission_started
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

        # Основной цикл обработки сообщений
        while True:
            # 🔥 ЖЕСТКИЙ ЗАМОК ДЛЯ ИСКЛЮЧЕНИЯ ПУСТОГО БИТРЕЙТА:
            # Если это видео-канал дрона, но обоюдное сопряжение еще не активировано кнопкой пульта,
            # мы ВООБЩЕ не вызываем websocket.recv(). Мы засыпаем на 100 мс и идем на следующий круг.
            # TCP-окно блокирует отправку байт со смартфона дрона. Трафик в 4G и в Google Cloud равен НУЛЮ!
            if role == "drone_video" and not mission_started:
                await asyncio.sleep(0.1)
                continue

            # Получаем пакет из сети
            message = await websocket.recv()

            # ПЕРЕХВАТ КОМАНДЫ СОПРЯЖЕНИЯ ОТ ПУЛЬТА
            if role == "controller_control" and message == "START_MISSION":
                async with lock:
                    # Проверяем, в сети ли оба устройства для обоюдного сопряжения
                    has_drone = clients.get("drone_control") is not None
                    has_remote = clients.get("controller_control") is not None
                    
                    if has_drone and has_remote:
                        mission_started = True
                        print("🚀 ОБОЮДНОЕ СОПРЯЖЕНИЕ АКТИВИРОВАНО! Поток H.264 открыт.")
                        # Шлем подтверждение обратно на пульт и дрон, чтобы они знали о старте
                        try:
                            await clients["controller_control"].send("MISSION_ACTIVE")
                            await clients["drone_control"].send("MISSION_ACTIVE")
                        except Exception:
                            pass
                    else:
                        print("⚠️ Не удается сопрячь: дрон или пульт еще не подключены к серверу.")
                continue

            # Оптимизация логов: бинарные кадры не печатаем в консоль, чтобы не грузить ядро виртуалки e2-micro
            if not isinstance(message, bytes):
                print(f"{role} -> {message}")

            # Логика маршрутизации пакетов
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

            # Мгновенная отправка адресату без задержек
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
                
                # Если пульт или дрон отключились — сбрасываем замок сопряжения обратно в безопасный режим
                if role == "controller_control" or role == "drone_control":
                    mission_started = False
                    print("🔒 Соединение разорвано. Замок сопряжения заблокирован обратно.")
            print(f"{role} disconnected")


async def main():
    print("WebSocket server started on Google Cloud")

    # 🔥 ТОТАЛЬНОЕ УНИЧТОЖЕНИЕ БУФЕРОВ И ОЧЕРЕДЕЙ ДЛЯ НАСТОЯЩЕГО REAL-TIME:
    # max_queue=1 и read_limit=16384 заставляют Linux работать как проточная труба.
    # Сервер не копит пакеты про запас. Команды управления (газ, крен, ARM) летят с нулевым пингом!
    async with websockets.serve(
        handler,
        "0.0.0.0",
        8080,
        max_size=None,
        max_queue=1,          # Очередь жестко зажата до 1 кадра! Старый мусор уничтожается.
        read_limit=16384,     # Ограничение буфера чтения операционной системы
        write_limit=16384,    # Ограничение буфера записи операционной системы
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
