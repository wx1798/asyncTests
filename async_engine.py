import asyncio
import websockets
import json
import sys
import time
from async_example import QAQueue

class async_engine():
    pass


class SerialDataProxy(object):
    """
    K线及Tick序列数据包装器, 方便数据读取使用

    Examples::

        # 获取一个分钟线序列, ks 即是 SerialDataProxy 的实例
        ks = api.get_kline_serial("SHFE.cu1812", 60)

        # 获取最后一根K线
        a = ks[-1]
        # 获取倒数第5根K线
        a = ks[-5]
        # a == {
        #     "datetime": ...,
        #     "open": ...,
        #     "high": ...,
        #     "low": ...,
        #     ...
        # }

        # 获取特定字段的序列
        cs = ks.close
        # cs = [3245, 3421, 3345, ...]

        # 将序列转为 pandas.DataFrame
        ks.to_dataframe()
    """

    def __init__(self, serial_root, width, default):
        self.serial_root = serial_root
        self.width = width
        self.default = default
        self.attr = list(self.default.keys())

    def __getattr__(self, name):
        return [self[i][name] for i in range(0, self.width)]

    def __getitem__(self, key):
        last_id = self.serial_root.get("last_id", None)
        if not last_id:
            return self.default.copy()
        if key < 0:
            data_id = last_id + 1 + key
        else:
            data_id = last_id - self.width + 1 + key
        return async_api._get_obj(self.serial_root, ["data", str(data_id)], self.default)

    def to_dataframe(self):
        """
        将当前该序列中的数据转换为 pandas.DataFrame

        Returns:
            pandas.DataFrame: 每行是一条行情数据

            注意: 返回的 DataFrame 反映的是当前的行情数据，不会自动更新，当行情数据有变化后需要重新调用 to_dataframe

        Example::

            # 判断K线是否为阳线
            from tqsdk.api import TqApi

            api = TqApi("SIM")
            k_serial = api.get_kline_serial("SHFE.cu1812", 60)
            while True:
                api.wait_update()
                df = k_serial.to_dataframe()
                print(df["close"] > df["open"])

            # 预计的输出是这样的:
            0       True
            1       True
            2      False
                   ...
            197    False
            198     True
            199    False
            Length: 200, dtype: bool
            ...
        """
        import pandas as pd
        rows = {}
        for i in range(0, self.width):
            rows[i] = {k: v for k, v in self[i].items()
                       if not k.startswith("_")}
        return pd.DataFrame.from_dict(rows, orient="index")





class async_api():

    def __init__(self, url):
        self.ws_url = url  # 天勤终端的地址
        self.loop = asyncio.new_event_loop()  # 创建一个新的ioloop, 避免和其他框架/环境产生干扰
        self.quote_symbols = set()  # 订阅的实时行情列表
        self.async_quue = QAQueue(self)
        # self.account_id = account_id  # 交易帐号id
        self.tasks = set()  # 由api维护的所有根task，不包含子task，子task由其父task维护
        if sys.platform.startswith("win"):
            # Windows系统下asyncio不支持KeyboardInterrupt的临时补丁
            self.create_task(self._windows_patch())
        self.create_task(self._connect())  # 启动websocket连接
        deadline = time.time() + 60

        self.send_chen=QAQueue(self)

        self.wait_update_chan = QAQueue(self, last_only=True)

    def wait_update(self, deadline=None):
        """
        等待业务数据更新

        调用此函数将阻塞当前线程, 等待天勤主进程发送业务数据更新并返回

        Args:
            deadline (float): [可选]指定截止时间，自unix epoch(1970-01-01 00:00:00 GMT)以来的秒数(time.time())。默认没有超时(无限等待)

        Returns:
            bool: 如果收到业务数据更新则返回 True, 如果到截止时间依然没有收到业务数据更新则返回 False

        注意: 由于存在网络延迟, 因此有数据更新不代表之前发出的所有请求都被处理了, 例如::

            from tqsdk.api import TqApi

            api = TqApi("SIM")
            quote = api.get_quote("SHFE.cu1812")
            api.wait_update()
            print(quote["datetime"])

            可能输出 ""(空字符串), 表示还没有收到该合约的行情
        """
        if self.loop.is_running():
            raise Exception(
                "不能在协程中调用 wait_update, 如需在协程中等待业务数据更新请使用 register_update_notify")
        self.diffs = []
        update_task = self.create_task(self.wait_update_chan.recv())
        try:
            while not update_task.done():
                timeout = None if deadline is None else max(
                    deadline - time.time(), 0)
                done, pending = self.loop.run_until_complete(asyncio.wait(
                    self.tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED))
                if len(done) == 0 and len(self.diffs) == 0:
                    return False
                self.tasks.difference_update(done)
                # 取出已完成任务的结果，如果执行过程中遇到例外会在这里抛出
                for t in done:
                    t.result()
            return True
        finally:
            update_task.cancel()
            self.tasks.discard(update_task)

    # ----------------------------------------------------------------------
    def is_changing(self, obj, key=None):
        """
        判定obj最近是否有更新

        当业务数据更新导致 wait_update 返回后可以使用该函数判断本次业务数据更新是否包含特定obj或其中某个字段

        Args:
            obj (any): 任意业务对象, 包括 get_quote 返回的 quote, get_kline_serial 返回的 k_serial, get_account 返回的 account 等

            key (str/list of str): [可选]需要判断的字段，默认不指定
                                  * 不指定: 当该obj下的任意字段有更新时返回True, 否则返回 False.
                                  * str: 当该obj下的指定字段有更新时返回True, 否则返回 False.
                                  * list of str: 当该obj下的指定字段中的任何一个字段有更新时返回True, 否则返回 False.

        Returns:
            bool: 如果本次业务数据更新包含了待判定的数据则返回 True, 否则返回 False.

        Example::

            # 追踪 SHFE.cu1812 的最新价更新
            from tqsdk.api import TqApi

            api = TqApi("SIM")
            quote = api.get_quote("SHFE.cu1812")
            while True:
                api.wait_update()
                if api.is_changing(quote, "last_price"):
                    print(quote["last_price"])

            # 以上代码运行后的输出是这样的:
            51800.0
            51810.0
            51800.0
            ...
        """
        if self.loop.is_running():
            raise Exception(
                "不能在协程中调用 is_changing, 如需在协程中判断业务数据更新请使用 register_update_notify")
        if obj is None:
            return False
        if not isinstance(key, list):
            key = [key] if key else []
        try:
            path = obj.serial_root["_path"] if isinstance(
                obj, SerialDataProxy) else obj["_path"]
        except KeyError:
            return False
        for diff in self.diffs:
            if self._is_key_exist(diff, path, key):
                return True
        return False

    # ----------------------------------------------------------------------
    def create_task(self, coro):
        """
        创建一个task

        一个task就是一个协程，task的调度是在 wait_update 函数中完成的，如果代码从来没有调用 wait_update，则task也得不到执行

        Args:
            coro (coroutine):  需要创建的协程

        Example::

            # 一个简单的task
            import asyncio
            from tqsdk.api import TqApi

            async def hello():
                await asyncio.sleep(3)
                print("hello world")

            api = TqApi("SIM")
            api.create_task(hello())
            while True:
                api.wait_update()

            #以上代码将在3秒后输出
            hello world
        """
        task = self.loop.create_task(coro)
        if asyncio.Task.current_task(loop=self.loop) is None:
            self.tasks.add(task)
        return task

    # ----------------------------------------------------------------------
    def register_update_notify(self, obj=None, chan=None):
        """
        注册一个channel以便接受业务数据更新通知

        调用此函数将返回一个channel, 当obj更新时会通知该channel

        推荐使用 async with api.register_update_notify() as update_chan 来注册更新通知

        如果直接调用 update_chan = api.register_update_notify() 则使用完成后需要调用 await update_chan.close() 避免资源泄漏

        Args:
            obj (any/list of any): [可选]任意业务对象, 包括 get_quote 返回的 quote, get_kline_serial 返回的 k_serial, get_account 返回的 account 等。默认不指定，监控所有业务对象

            chan (TqChan): [可选]指定需要注册的channel。默认不指定，由本函数创建

        Example::

            # 获取 SHFE.cu1812 合约的报价
            from tqsdk.api import TqApi

            async def demo():
                quote = api.get_quote("SHFE.cu1812")
                async with api.register_update_notify(quote) as update_chan:
                    async for _ in update_chan:
                        print(quote["last_price"])

            api = TqApi("SIM")
            api.create_task(demo())
            while True:
                api.wait_update()

            #以上代码将输出
            nan
            51850.0
            51850.0
            51690.0
            ...
        """
        if chan is None:
            chan = TqChan(self, last_only=True)
        if not isinstance(obj, list):
            obj = [obj] if obj else [self.data]
        for o in obj:
            listener = o.serial_root["_listener"] if isinstance(
                o, SerialDataProxy) else o["_listener"]
            listener.add(chan)
        return chan

    # ----------------------------------------------------------------------
    async def _windows_patch(self):
        """Windows系统下asyncio不支持KeyboardInterrupt的临时补丁, 详见 https://bugs.python.org/issue23057"""
        while True:
            await asyncio.sleep(1)

    async def _connect(self):
        """启动websocket客户端"""
        async with websockets.connect(self.ws_url, max_size=None) as client:
            send_task = self.create_task(self._send_handler(client))
            try:
                async for msg in client:
                    self._on_receive_msg(msg)
            except websockets.exceptions.ConnectionClosed:
                print("网络连接断开，请检查客户端及网络是否正常", file=sys.stderr)
                raise
            finally:
                send_task.cancel()

    async def _send_handler(self, client):
        """websocket客户端数据发送协程"""
        async for msg in self.send_chan:
            await client.send(msg)
            self.logger.debug("message sent: %s", msg)

    def _send_json(self, obj):
        """向天勤主进程发送JSON包"""
        self.send_chan.send_nowait(json.dumps(obj))

    def _on_receive_msg(self, msg):
        """收到数据推送"""
        pack = json.loads(msg)
        data = pack.get("data", [])
        self.diffs.extend(data)
        for d in data:
            self._merge_diff(self.data, d, self.prototype)


    @staticmethod
    def _merge_diff(result, diff, prototype):
        """更新业务数据,并同步发送更新通知，保证业务数据的更新和通知是原子操作"""
        for key in list(diff.keys()):
            if isinstance(diff[key], str) and key in prototype and not isinstance(prototype[key], str):
                diff[key] = prototype[key]
            if diff[key] is None:
                dv = result.pop(key, None)
                TqApi._notify_update(dv, True)
            elif isinstance(diff[key], dict):
                target = TqApi._get_obj(result, [key])
                tpt = prototype.get("*", {})
                if key in prototype:
                    tpt = prototype[key]
                TqApi._merge_diff(target, diff[key], tpt)
                if len(diff[key]) == 0:
                    del diff[key]
            elif key in result and result[key] == diff[key]:
                del diff[key]
            else:
                result[key] = diff[key]
        if len(diff) != 0:
            TqApi._notify_update(result, False)

    @staticmethod
    def _notify_update(target, recursive):
        """同步通知业务数据更新"""
        if isinstance(target, dict):
            target["_listener"] = {q for q in target["_listener"] if not q.closed}
            for q in target["_listener"]:
                q.send_nowait(True)
            if recursive:
                for v in target.values():
                    TqApi._notify_update(v, recursive)

    @staticmethod
    def _get_obj(root, path, default=None):
        """获取业务数据"""
        # todo: support nested dict for default value
        d = root
        for i in range(len(path)):
            if path[i] not in d:
                dv = {} if i != len(path) - 1 or default is None else copy.copy(default)
                if isinstance(dv, dict):
                    dv["_path"] = d["_path"] + [path[i]]
                    dv["_listener"] = set()
                d[path[i]] = dv
            d = d[path[i]]
        return d

    @staticmethod
    def _is_key_exist(diff, path, key):
        """判断指定数据是否存在"""
        for p in path:
            if not isinstance(diff, dict) or p not in diff:
                return False
            diff = diff[p]
        if not isinstance(diff, dict):
            return False
        for k in key:
            if k in diff:
                return True
        return len(key) == 0

    @staticmethod
    def _gen_quote_prototype():
        """行情的数据原型"""
        return {
            "datetime": "",  # "2017-07-26 23:04:21.000001" (行情从交易所发出的时间(北京时间))
            "ask_price1": float("nan"),  # 6122.0 (卖一价)
            "ask_volume1": 0,  # 3 (卖一量)
            "bid_price1": float("nan"),  # 6121.0 (买一价)
            "bid_volume1": 0,  # 7 (买一量)
            "last_price": float("nan"),  # 6122.0 (最新价)
            "highest": float("nan"),  # 6129.0 (当日最高价)
            "lowest": float("nan"),  # 6101.0 (当日最低价)
            "open": float("nan"),  # 6102.0 (开盘价)
            "close": float("nan"),  # nan (收盘价)
            "average": float("nan"),  # 6119.0 (当日均价)
            "volume": 0,  # 89252 (成交量)
            "amount": float("nan"),  # 5461329880.0 (成交额)
            "open_interest": 0,  # 616424 (持仓量)
            "settlement": float("nan"),  # nan (结算价)
            "upper_limit": float("nan"),  # 6388.0 (涨停价)
            "lower_limit": float("nan"),  # 5896.0 (跌停价)
            "pre_open_interest": 0,  # 616620 (昨持仓量)
            "pre_settlement": float("nan"),  # 6142.0 (昨结算价)
            "pre_close": float("nan"),  # 6106.0 (昨收盘价)
            "price_tick": float("nan"),  # 10.0 (合约价格单位)
            "price_decs": 0,  # 0 (合约价格小数位数)
            "volume_multiple": 0,  # 10 (合约乘数)
            "max_limit_order_volume": 0,  # 500 (最大限价单手数)
            "max_market_order_volume": 0,  # 0 (最大市价单手数)
            "min_limit_order_volume": 0,  # 1 (最小限价单手数)
            "min_market_order_volume": 0,  # 0 (最小市价单手数)
            "underlying_symbol": "",  # SHFE.rb1901 (标的合约)
            "strike_price": float("nan"),  # nan (行权价)
            "change": float("nan"),  # −20.0 (涨跌)
            "change_percent": float("nan"),  # −0.00325 (涨跌幅)
            "expired": False,  # False (合约是否已下市)
        }

    @staticmethod
    def _gen_kline_prototype():
        """K线的数据原型"""
        return {
            "datetime": 0,  # 1501080715000000000 (K线起点时间(按北京时间)，自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数)
            "open": float("nan"),  # 51450.0 (K线起始时刻的最新价)
            "high": float("nan"),  # 51450.0 (K线时间范围内的最高价)
            "low": float("nan"),  # 51450.0 (K线时间范围内的最低价)
            "close": float("nan"),  # 51450.0 (K线结束时刻的最新价)
            "volume": 0,  # 11 (K线时间范围内的成交量)
            "open_oi": 0,  # 27354 (K线起始时刻的持仓量)
            "close_oi": 0,  # 27355 (K线结束时刻的持仓量)
        }

    @staticmethod
    def _gen_tick_prototype():
        """Tick的数据原型"""
        return {
            "datetime": 0,  # 1501074872000000000 (tick从交易所发出的时间(按北京时间)，自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数)
            "last_price": float("nan"),  # 3887.0 (最新价)
            "average": float("nan"),  # 3820.0 (当日均价)
            "highest": float("nan"),  # 3897.0 (当日最高价)
            "lowest": float("nan"),  # 3806.0 (当日最低价)
            "ask_price1": float("nan"),  # 3886.0 (卖一价)
            "ask_volume1": 0,  # 3 (卖一量)
            "bid_price1": float("nan"),  # 3881.0 (买一价)
            "bid_volume1": 0,  # 18 (买一量)
            "volume": 0,  # 7823 (当日成交量)
            "amount": float("nan"),  # 19237841.0 (成交额)
            "open_interest": 0,  # 1941 (持仓量)
        }

    @staticmethod
    def _gen_account_prototype():
        """账户的数据原型"""
        return {
            "currency": "",  # "CNY" (币种)
            "pre_balance": float("nan"),  # 9912934.78 (昨日账户权益)
            "static_balance":float("nan"),  # (静态权益)
            "balance": float("nan"),  # 9963216.55 (账户权益)
            "available": float("nan"),  # 9480176.15 (可用资金)
            "float_profit": float("nan"),  # 8910.0 (浮动盈亏)
            "position_profit": float("nan"),  # 1120.0(持仓盈亏)
            "close_profit": float("nan"),  # -11120.0 (本交易日内平仓盈亏)
            "frozen_margin": float("nan"),  # 0.0(冻结保证金)
            "margin": float("nan"),  # 11232.23 (保证金占用)
            "frozen_commission": float("nan"),  # 0.0 (冻结手续费)
            "commission": float("nan"),  # 123.0 (本交易日内交纳的手续费)
            "frozen_premium": float("nan"),  # 0.0 (冻结权利金)
            "premium": float("nan"),  # 0.0 (本交易日内交纳的权利金)
            "deposit": float("nan"),  # 1234.0 (本交易日内的入金金额)
            "withdraw": float("nan"),  # 890.0 (本交易日内的出金金额)
            "risk_ratio": float("nan"),  # 0.048482375 (风险度)
        }

    @staticmethod
    def _gen_order_prototype():
        """委托单的数据原型"""
        return {
            "order_id": "",  # "123" (委托单ID, 对于一个用户的所有委托单，这个ID都是不重复的)
            "exchange_order_id":"",  # "1928341" (交易所单号)
            "exchange_id": "",  # "SHFE" (交易所)
            "instrument_id": "",  # "rb1901" (交易所内的合约代码)
            "direction": "",  # "BUY" (下单方向, BUY=买, SELL=卖)
            "offset": "",  # "OPEN" (开平标志, OPEN=开仓, CLOSE=平仓, CLOSETODAY=平今)
            "volume_orign":0,  # 10 (总报单手数)
            "volume_left":0,  # 5 (未成交手数)
            "limit_price": float("nan"),  # 4500.0 (委托价格, 仅当 price_type = LIMIT 时有效)
            "price_type": "",  # "LIMIT" (价格类型, ANY=市价, LIMIT=限价)
            "volume_condition": "",  # "ANY" (手数条件, ANY=任何数量, MIN=最小数量, ALL=全部数量)
            "time_condition": "",  # "GFD" (时间条件, IOC=立即完成，否则撤销, GFS=本节有效, GFD=当日有效, GTC=撤销前有效, GFA=集合竞价有效)
            "insert_date_time": 0,  # 1501074872000000000 (下单时间(按北京时间)，自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数)
            "last_msg":"",  # "报单成功" (委托单状态信息)
            "status": "",  # "ALIVE" (委托单状态, ALIVE=有效, FINISHED=已完)
        }

    @staticmethod
    def _gen_trade_prototype():
        """成交的数据原型"""
        return {
            "order_id": "",  # "123" (委托单ID, 对于一个用户的所有委托单，这个ID都是不重复的)
            "trade_id": "",  # "123|19723" (成交ID, 对于一个用户的所有成交，这个ID都是不重复的)
            "exchange_trade_id":"",  # "829414" (交易所成交号)
            "exchange_id": "",  # "SHFE" (交易所)
            "instrument_id": "",  # "rb1901" (交易所内的合约代码)
            "direction": "",  # "BUY" (下单方向, BUY=买, SELL=卖)
            "offset": "",  # "OPEN" (开平标志, OPEN=开仓, CLOSE=平仓, CLOSETODAY=平今)
            "price": float("nan"),  # 4510.0 (成交价格)
            "volume": 0,  # 5 (成交手数)
            "trade_date_time": 0,  # 1501074872000000000 (成交时间(按北京时间)，自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数)
        }

    @staticmethod
    def _gen_position_prototype():
        """持仓的数据原型"""
        return {
            "exchange_id": "",  # "SHFE" (交易所)
            "instrument_id": "",  # "rb1901" (交易所内的合约代码)
            "volume_long_today": 0,  # 10 (多头今仓手数)
            "volume_long_his": 0,  # 5 (多头老仓手数)
            "volume_long": 0,  # 15 (多头手数)
            "volume_long_frozen_today": 0,  # 3 (多头今仓冻结)
            "volume_long_frozen_his": 0,  # 2 (多头老仓冻结)
            "volume_long_frozen": 0,  # 5 (多头持仓冻结)
            "volume_short_today": 0,  # 3 (空头今仓手数)
            "volume_short_his": 0,  # 0 (空头老仓手数)
            "volume_short": 0,  # 3 (空头手数)
            "volume_short_frozen_today": 0,  # 0 (空头今仓冻结)
            "volume_short_frozen_his": 0,  # 0 (空头老仓冻结)
            "volume_short_frozen": 0,  # 0 (空头持仓冻结)
            "open_price_long": float("nan"),  # 3120.0 (多头开仓均价)
            "open_price_short": float("nan"),  # 3310.0 (空头开仓均价)
            "open_cost_long": float("nan"),  # 468000.0 (多头开仓市值)
            "open_cost_short": float("nan"),  # 99300.0 (空头开仓市值)
            "position_price_long": float("nan"),  # 3200.0 (多头持仓均价)
            "position_price_short": float("nan"),  # 3330.0 (空头持仓均价)
            "position_cost_long": float("nan"),  # 480000.0 (多头持仓市值)
            "position_cost_short": float("nan"),  # 99900.0 (空头持仓市值)
            "float_profit_long": float("nan"),  # 12000.0 (多头浮动盈亏)
            "float_profit_short": float("nan"),  # 3300.0 (空头浮动盈亏)
            "float_profit": float("nan"),  # 15300.0 (浮动盈亏)
            "position_profit_long": float("nan"),  # 0.0 (多头持仓盈亏)
            "position_profit_short": float("nan"),  # 3900.0 (空头持仓盈亏)
            "position_profit": float("nan"),  # 3900.0 (持仓盈亏)
            "margin_long": float("nan"),  # 50000.0 (多头占用保证金)
            "margin_short": float("nan"),  # 10000.0 (空头占用保证金)
            "margin": float("nan"),  # 60000.0 (占用保证金)
        }

    @staticmethod
    def _generate_chart_id(symbol, duration_seconds):
        """生成chart id"""
        chart_id = "PYSDK_" + uuid.uuid4().hex
        return chart_id

    @staticmethod
    def _generate_order_id():
        """生成order id"""
        return uuid.uuid4().hex