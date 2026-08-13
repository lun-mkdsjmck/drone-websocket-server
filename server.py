import asyncio
import websockets

clients = {
    "controller_control": None,
    "drone_control": None,
    "controller_video": None,
    "drone_video": None
}

mission_started = False
lock = asyncio.Lock()


# ============================================================
# ВИДЕО: очередь только последнего кадра
# ============================================================

video_queue = None
video_sender_task = None


async def video_sender():
    global video_queue

    while True:
        frame = await video_queue.get()

        async with lock:
            target = clients.get("controller_video")

        # Пульта нет — кадр просто выбрасываем
        if target is None:
            continue

        try:
            await target.send(frame)
        except Exception:
            pass


# ============================================================
# ДОБАВИТЬ КАДР В ВИДЕОПОТОК
# ============================================================

async def push_video_frame(frame):

    global video_queue

    # Пульт и дрон должны быть подключены
    async with lock:
        drone = clients.get("drone_video")
        controller = clients.get("controller_video")
        active = mission_started

    if drone is None:
        return

    if controller is None:
        return

    if not active:
        return

    # --------------------------------------------------------
    # В очереди НИКОГДА не должно быть больше одного кадра.
    #
    # Если там уже лежит старый кадр —
    # забираем его и выбрасываем.
    # --------------------------------------------------------

    try:
        while not video_queue.empty():
            try:
                video_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        video_queue.put_nowait(frame)

    except asyncio.QueueFull:
        pass


# ============================================================
# ОСНОВНОЙ HANDLER
# ============================================================

async def handler(websocket):

    global mission_started
    global video_queue
    global video_sender_task

    role = None

    try:

        # ----------------------------------------------------
        # Регистрация роли
        # ----------------------------------------------------

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
            await websocket.close()
            return

        # ----------------------------------------------------
        # Регистрируем соединение
        # ----------------------------------------------------

        async with lock:

            old_client = clients.get(role)

            if old_client is not None:
                try:
                    await old_client.close()
                except Exception:
                    pass

            clients[role] = websocket

        print(f"{role} connected")

        # ----------------------------------------------------
        # Запускаем один видеосендер
        # ----------------------------------------------------

        if video_queue is None:
            video_queue = asyncio.Queue(maxsize=1)

        if video_sender_task is None or video_sender_task.done():
            video_sender_task = asyncio.create_task(
                video_sender()
            )

        # ====================================================
        # ОСНОВНОЙ ЦИКЛ
        # ====================================================

        while True:

            message = await websocket.recv()

            # =================================================
            # DRONE VIDEO
            # =================================================

            if role == "drone_video":

                # Видеоканал принимает ТОЛЬКО bytes
                if not isinstance(message, bytes):
                    continue

                # ------------------------------------------------
                # Если нет полного сопряжения:
                #
                # КАДР СРАЗУ ВЫБРАСЫВАЕМ.
                #
                # Никакого накопления.
                # ------------------------------------------------

                async with lock:

                    controller_exists = (
                        clients.get("controller_video") is not None
                    )

                    active = mission_started

                if not controller_exists or not active:
                    continue

                # ------------------------------------------------
                # Передаём только последний кадр
                # ------------------------------------------------

                await push_video_frame(message)

                continue

            # =================================================
            # CONTROLLER VIDEO
            # =================================================

            if role == "controller_video":

                # Видеоканал пульта принимает ТОЛЬКО команды
                # START/STOP. Никакого другого трафика.
                if isinstance(message, bytes):
                    continue

                if message == "START_MISSION":

                    async with lock:

                        has_drone = (
                            clients.get("drone_control") is not None
                            and
                            clients.get("drone_video") is not None
                        )

                        has_controller = (
                            clients.get("controller_control") is not None
                            and
                            clients.get("controller_video") is not None
                        )

                        if has_drone and has_controller:

                            mission_started = True

                            print(
                                "MISSION ACTIVE"
                            )

                            try:

                                controller_control = clients.get(
                                    "controller_control"
                                )

                                drone_control = clients.get(
                                    "drone_control"
                                )

                                if controller_control:
                                    await controller_control.send(
                                        "MISSION_ACTIVE"
                                    )

                                if drone_control:
                                    await drone_control.send(
                                        "MISSION_ACTIVE"
                                    )

                            except Exception:
                                pass

                    continue

                if message == "STOP_MISSION":

                    async with lock:

                        mission_started = False

                    # ------------------------------------------------
                    # Немедленно очистить старый кадр.
                    # ------------------------------------------------

                    if video_queue is not None:

                        while not video_queue.empty():

                            try:
                                video_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                break

                    print(
                        "MISSION STOPPED"
                    )

                    async with lock:

                        try:

                            controller_control = clients.get(
                                "controller_control"
                            )

                            drone_control = clients.get(
                                "drone_control"
                            )

                            if controller_control:
                                await controller_control.send(
                                    "MISSION_STOPPED"
                                )

                            if drone_control:
                                await drone_control.send(
                                    "MISSION_STOPPED"
                                )

                        except Exception:
                            pass

                    continue

                # Любой другой текст через controller_video
                # игнорируется.
                continue

            # =================================================
            # CONTROL
            # =================================================

            if role == "controller_control":

                if not isinstance(message, str):
                    continue

                target_role = "drone_control"

            elif role == "drone_control":

                if not isinstance(message, str):
                    continue

                target_role = "controller_control"

            else:
                continue

            # =================================================
            # Передача команд
            # =================================================

            async with lock:
                target = clients.get(target_role)

            if target is not None:

                try:
                    await target.send(message)

                except Exception:
                    pass

    except websockets.ConnectionClosed:
        pass

    except Exception as e:
        print(f"{role}: {e}")

    finally:

        if role:

            async with lock:

                if clients.get(role) == websocket:
                    clients[role] = None

                # ---------------------------------------------
                # Если исчез любой управляющий канал —
                # миссия прекращается.
                # ---------------------------------------------

                if role in (
                    "controller_control",
                    "drone_control"
                ):
                    mission_started = False

            # ---------------------------------------------
            # Если исчез видеоприёмник —
            # сразу прекращаем видеопоток.
            # ---------------------------------------------

            if role == "controller_video":

                async with lock:
                    mission_started = False

                if video_queue is not None:

                    while not video_queue.empty():

                        try:
                            video_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

            # ---------------------------------------------
            # Если исчез дрон-видео —
            # тоже очищаем очередь.
            # ---------------------------------------------

            if role == "drone_video":

                if video_queue is not None:

                    while not video_queue.empty():

                        try:
                            video_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

            print(f"{role} disconnected")


# ============================================================
# SERVER
# ============================================================

async def main():

    print("WebSocket server started on Google Cloud")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        8080,

        # H.264 кадры могут быть любого размера
        max_size=None,

        # Небольшая входная очередь
        max_queue=1,

        read_limit=16384,
        write_limit=16384,

        ping_interval=20,
        ping_timeout=20,
    ):

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
