#

import asyncio


class QAQueue(asyncio.Queue):
    def __init__(self, api, last_only=False):
        asyncio.Queue.__init__(self, loop=api.loop)
        self.last_only = last_only
        self.closed = False

    async def close(self):
        """
        关闭channel

        关闭后send将不起作用,recv在收完剩余数据后会立即返回None
        """
        self.closed = True
        await asyncio.Queue.put(self, None)

    async def send(self, item):
        """
        异步发送数据到channel中

        Args:
            item (any): 待发送的对象
        """
        if not self.closed:
            if self.last_only:
                while not self.empty():
                    asyncio.Queue.get_nowait(self)
            await asyncio.Queue.put(self, item)

    def send_nowait(self, item):
        """
        尝试立即发送数据到channel中

        Args:
            item (any): 待发送的对象

        Raises:
            asyncio.QueueFull: 如果channel已满则会抛出 asyncio.QueueFull
        """
        if not self.closed:
            if self.last_only:
                while not self.empty():
                    asyncio.Queue.get_nowait(self)
            asyncio.Queue.put_nowait(self, item)

    async def recv(self):
        """
        异步接收channel中的数据，如果channel中没有数据则一直等待

        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        """
        if self.closed and self.empty():
            return None
        return await asyncio.Queue.get(self)

    def recv_nowait(self):
        """
        尝试立即接收channel中的数据

        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None

        Raises:
            asyncio.QueueFull: 如果channel中没有数据则会抛出 asyncio.QueueEmpty
        """
        if self.closed and self.empty():
            return None
        return asyncio.Queue.get_nowait(self)

    def recv_latest(self, latest):
        """
        尝试立即接收channel中的最后一个数据

        Args:
            latest (any): 如果当前channel中没有数据或已关闭则返回该对象

        Returns:
            any: channel中的最后一个数据
        """
        while (self.closed and self.qsize() > 1) or (not self.closed and not self.empty()):
            latest = asyncio.Queue.get_nowait(self)
        return latest

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await asyncio.Queue.get(self)
        if self.closed and self.empty():
            raise StopAsyncIteration
        return value

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()




class test_api():
    def __init__(self):
        self.loop=asyncio.new_event_loop()
        self.queue=QAQueue(self)

    def test(self):
        self.queue.send_nowait(1)
        print(self.queue.get_nowait())


    async def _send_handler(self, client):
        """websocket客户端数据发送协程"""
        async for msg in self.queue:
            await client.send(msg)
            #self.logger.debug("message sent: %s", msg)

test_api().test()


    # async def _run(self):
    #     """负责下单的task"""
    #     order = self.api.insert_order(self.symbol, self.direction, self.offset, self.volume, self.limit_price)
    #     last_order = order.copy()
    #     last_left = 0
    #     async with self.api.register_update_notify() as update_chan:
    #         await self.order_chan.send(last_order)
    #         while order["status"] != "FINISHED":
    #             await update_chan.recv()
    #             if order["volume_left"] != last_left:
    #                 vol = last_left - order["volume_left"]
    #                 last_left = order["volume_left"]
    #                 await self.trade_chan.send(vol if order["direction"] == "BUY" else -vol)
    #             if order != last_order:
    #                 last_order = order.copy()
    #                 await self.order_chan.send(last_order)
