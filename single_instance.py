import os
from pathlib import Path


class SingleInstanceError(RuntimeError):
    """Вторая копия процесса уже удерживает lock."""


class SingleInstanceLock:
    """
    Межпроцессный lock для runner.

    Windows:
        msvcrt.locking — lock автоматически освобождается ОС
        при завершении/падении процесса.

    Linux/macOS:
        fcntl.flock — нужен только для локальных тестов проекта.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._file = None
        self._backend = None

    def acquire(self):
        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._file = open(
            self.path,
            "a+b",
        )

        self._file.seek(0, os.SEEK_END)
        if self._file.tell() == 0:
            self._file.write(b"0")
            self._file.flush()

        self._file.seek(0)

        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(
                    self._file.fileno(),
                    msvcrt.LK_NBLCK,
                    1,
                )
            except OSError as error:
                self._file.close()
                self._file = None
                raise SingleInstanceError(
                    f"Другая копия процесса уже запущена (lock: {self.path})."
                ) from error

            self._backend = "msvcrt"

        else:
            import fcntl

            try:
                fcntl.flock(
                    self._file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except OSError as error:
                self._file.close()
                self._file = None
                raise SingleInstanceError(
                    f"Другая копия процесса уже запущена (lock: {self.path})."
                ) from error

            self._backend = "fcntl"

        # PID пишем после захвата lock. Первый байт оставляем под lock.
        payload = f"\nPID={os.getpid()}\n".encode("utf-8")
        self._file.seek(1)
        self._file.truncate()
        self._file.write(payload)
        self._file.flush()

        return self

    def release(self):
        if self._file is None:
            return

        try:
            self._file.seek(0)

            if self._backend == "msvcrt":
                import msvcrt

                try:
                    msvcrt.locking(
                        self._file.fileno(),
                        msvcrt.LK_UNLCK,
                        1,
                    )
                except OSError:
                    pass

            elif self._backend == "fcntl":
                import fcntl

                try:
                    fcntl.flock(
                        self._file.fileno(),
                        fcntl.LOCK_UN,
                    )
                except OSError:
                    pass

        finally:
            self._file.close()
            self._file = None
            self._backend = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
