import time
import asyncio
from typing import Coroutine, Any, Generator, Callable


class TimedAwaitable:
    """包装一个协程，自动测量排除 await 等待后的实际运算时间。

    Parameters
    ----------
    coro:
        要计时的协程对象。
    clock:
        计时器，默认 ``time.perf_counter``（高精度墙上时间）。
        可传入 ``time.process_time`` 仅计量 CPU 时间。
        也可传入任意 ``() -> float`` 的可调用对象。
    """

    def __init__(
        self,
        coro: Coroutine[Any, Any, Any],
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._coro: Coroutine[Any, Any, Any] = coro
        self._clock: Callable[[], float] = clock
        self.elapsed: float = 0.0  # 累计的活跃运算时间（秒）

    def __await__(self) -> Generator[Any, None, Any]:
        _coro: Generator[Any, Any, Any] = self._coro.__await__()
        _clock: Callable[[], float] = self._clock
        _start: float = _clock()
        _result: Any = None
        _exc: BaseException | None = None

        while True:
            # --- 一次 send/throw 开始 ---
            try:
                if _exc is not None:
                    _val: Any = _coro.throw(_exc)
                else:
                    _val = _coro.send(_result)
            except StopIteration as e:
                # 协程正常结束
                self.elapsed += _clock() - _start
                return e.value

            # 协程尚未结束，yield 了一个值（通常是 Future）
            self.elapsed += _clock() - _start

            # --- 将控制权交还给事件循环 ---
            try:
                _result = yield _val
            except GeneratorExit:
                _coro.close()
                raise
            except Exception as e:
                _exc = e
            else:
                _exc = None
            finally:
                _start = _clock()

async def compute(x: int) -> float:
    s = 0.0
    for i in range(x):
        s += i ** 0.5
    await asyncio.sleep(1)   # 这 1 秒不会被计入 CPU 时间或墙上时间的活跃运算时间
    time.sleep(1)          # 这 1 秒会被计入墙上时间，但不计入 CPU 时间
    s += 1
    return s

async def main() -> None:
    N = 10_000_000

    # 默认：墙上时间 (perf_counter)
    timed_wall = TimedAwaitable(compute(N))
    result = await timed_wall
    print(f"[墙上时间] 结果: {result:.6f},  耗时: {timed_wall.elapsed:.6f} 秒")

    # CPU 时间 (process_time)
    timed_cpu = TimedAwaitable(compute(N), clock=time.process_time)
    result = await timed_cpu
    print(f"[CPU 时间] 结果: {result:.6f},  耗时: {timed_cpu.elapsed:.6f} 秒")


asyncio.run(main())