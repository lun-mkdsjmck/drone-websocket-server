import asyncio
import websockets

clients = {
    "controller_control": None,
    "drone_control": None,
    "controller_video": None,
    "drone_video": None
}

# Переменная замка сопряжения
# По умолчанию миссия не начата, трафик заблокирован
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

        # =========================================================
        # ОСНОВНОЙ ЦИКЛ
        # =========================================================

        while True:

            # Если видео дрона еще не разрешено,
            # ждем активации миссии.
            if role == "drone_video" and not mission_started:
                await asyncio.sleep(0.1)
                continue

            # Получаем пакет из сети
            message = await websocket.recv()

            # =====================================================
            # КОМАНДЫ СИНХРОНИЗАЦИИ ОТ ПУЛЬТА
            # =====================================================

            if role == "controller_video":

                if message == "START_MISSION":

                    async with lock:

                        has_drone = (
                            clients.get("drone_control") is not None
                            or
                            clients.get("drone_video") is not None
                        )

                        has_remote = (
                            clients.get("controller_control") is not None
                            or
                            clients.get("controller_video") is not None
                        )

                        if has_drone and has_remote:

                            mission_started = True

                            print(
                                "🚀 ОБОЮДНОЕ СОПРЯЖЕНИЕ АКТИВИРОВАНО! "
                                "Поток H.264 открыт."
                            )

                            try:
                                if clients.get("controller_control"):
                                    await clients[
                                        "controller_control"
                                    ].send("MISSION_ACTIVE")

                                if clients.get("drone_control"):
                                    await clients[
                                        "drone_control"
                                    ].send("MISSION_ACTIVE")

                            except Exception:
                                pass

                        else:

                            print(
                                "⚠️ Не удается сопрячь: "
                                "дрон или пульт еще не подключены."
                            )

                    continue

                elif message == "STOP_MISSION":

                    async with lock:

                        mission_started = False

                        print(
                            "🔒 ПОТОК ПРИНУДИТЕЛЬНО ЗАКРЫТ ПИЛОТОМ! "
                            "Трафик остановлен."
                        )

                        try:

                            if clients.get("controller_control"):
                                await clients[
                                    "controller_control"
                                ].send("MISSION_STOPPED")

                            if clients.get("drone_control"):
                                await clients[
                                    "drone_control"
                                ].send("MISSION_STOPPED")

                        except Exception:
                            pass

                    continue

            # =====================================================
            # ЛОГИ
            # =====================================================

            # Бинарные видеокадры НЕ печатаем.
            if not isinstance(message, bytes):
                print(f"{role} -> {message}")

            # =====================================================
            # ЛОГИКА МАРШРУТИЗАЦИИ ПАКЕТОВ
            # ИСПРАВЛЕННАЯ: ОДНА ЦЕПОЧКА IF / ELIF
            # =====================================================

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

            # =====================================================
            # ПОЛУЧАЕМ АДРЕСАТА
            # =====================================================

            async with lock:
                target = clients.get(target_role)

            # =====================================================
            # ОТПРАВКА АДРЕСАТУ
            # =====================================================

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

                # Если пульт или дрон управления отключились,
                # сбрасываем замок сопряжения.
                if (
                    role == "controller_control"
                    or
                    role == "drone_control"
                ):

                    mission_started = False

                    print(
                        "🔒 Соединение разорвано. "
                        "Замок сопряжения заблокирован обратно."
                    )

            print(f"{role} disconnected")


async def main():

    print("WebSocket server started on Google Cloud")

    # =============================================================
    # WEBSOCKET SERVER
    # =============================================================

    async with websockets.serve(
        handler,
        "0.0.0.0",
        8080,

        # Не ограничиваем размер H.264 пакета
        max_size=None,

        # Максимально маленькая очередь
        max_queue=1,

        # Буферы чтения/записи
        read_limit=16384,
        write_limit=16384,

        # WebSocket keep-alive
        ping_interval=20,
        ping_timeout=20,
    ):

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
