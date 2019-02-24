
from dateutil import parser
import zlib
import re
from hyperquant.api import Platform, Interval, Direction
from hyperquant.clients import Endpoint, WSClient, Trade, ParamName, \
    Candle, WSConverter, RESTConverter, PrivatePlatformRESTClient


class OkexRESTConverterV1(RESTConverter):
    base_url = "https://www.okex.com/api/v{version}/"

    endpoint_lookup = {
        Endpoint.TRADE_HISTORY: "trades.do",
        Endpoint.CANDLE: "kline.do",
    }
    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.INTERVAL: "type",
        ParamName.LIMIT: "size",
    }
    param_lookup_by_class = {
        Trade: {
            "date": ParamName.TIMESTAMP,
            # "date_ms": ParamName.TIMESTAMP, # in microseconds
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "tid": ParamName.ITEM_ID,
            "type": ParamName.DIRECTION,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT,
        ],
    }

    is_source_in_microseconds = True

    param_value_lookup = {
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5m",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",
    }

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)
        # convert microseconds to milliseconds
        if result and isinstance(result, Candle):
            result.timestamp = result.timestamp/1000
        return result


class OkexRESTClient(PrivatePlatformRESTClient):
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OkexRESTConverterV1,
        }

    def fetch_trades_history(self, symbol, limit=None, from_item=None, to_item=None,
                             sorting=None, is_use_max_limit=False, from_time=None, to_time=None,
                             version=None, **kwargs):
        return self.fetch_history(Endpoint.TRADE, symbol, None, from_item, to_item,
                                  sorting, is_use_max_limit, from_time, to_time,
                                  version, **kwargs)[:limit]

    def fetch_candles(self, symbol, interval, limit=None, from_time=None, to_time=None,
                      is_use_max_limit=False, version=None, **kwargs):
        return super().fetch_candles(symbol, interval, limit, from_time, to_time,
                                     is_use_max_limit, version, **kwargs)


class OkexWSConverterV1(WSConverter):
    base_url = "wss://real.okex.com:10440/ws/v1"
    # trade is in string time but candle is not
    # so need to handle them separately
    # is_source_in_timestring = True
    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    param_lookup_by_class = {
        Trade: [
            ParamName.ITEM_ID,
            ParamName.PRICE,
            ParamName.AMOUNT,
            ParamName.TIMESTAMP,
            ParamName.ORDER_TYPE,
            ParamName.SYMBOL  # added manually
        ],
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            None,  # amount is not volume ??? ParamName.AMOUNT,
            ParamName.SYMBOL  # added manually
        ],
    }
    param_value_lookup = {
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5m",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "day",
        Interval.DAY_3: "3day",
        Interval.WEEK_1: "week",

        ParamName.DIRECTION: {
            Direction.SELL: "ask",
            Direction.BUY: "bid",
        },
    }

    def _generate_subscription(self, endpoint, symbol=None, **params):
        channel = super()._generate_subscription(endpoint, symbol, **params)
        _, params = self.prepare_params(None, params)
        channel_value = str()
        if channel == Endpoint.TRADE:
            channel_value = "ok_sub_spot_{}_deals".format(symbol)
        elif channel == Endpoint.CANDLE and 'interval' in params:
            channel_value = "ok_sub_spot_{0}_kline_{1}".format(symbol, params['interval'])
        return channel_value

    def parse(self, endpoint, data):
        # skip info packets
        if "binary" in data:
            if isinstance(data["binary"], int) and data["binary"]:
                return
        if data and "channel" in data:
            res = re.search("ok_sub_spot_(.+?)_deals", data["channel"])
            if res:
                endpoint = Endpoint.TRADE
                symbol = res.group(1)
                data = data["data"]
                data[0].append(symbol)
                # convers string timestamp tp and unix timestamp
                data[0][3] = int(parser.parse(data[0][3]).timestamp())
            else:
                res = re.search("ok_sub_spot_(.+?)_kline_(.+?)", data["channel"])
                if res:
                    endpoint = Endpoint.CANDLE
                    symbol = res.group(1)
                    data = data["data"]
                    data[0].append(symbol)
                    # convers to milliseconds and int
                    data[0][0] = int(data[0][0])/1000

        # WARNING: FIXME: it gets nested list from ws api remove one of them
        return super().parse(endpoint, data)[0]


class OkexWSClient(WSClient):
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OkexWSConverterV1,
    }

    def _send_subscribe(self, subscriptions):
        subscriptions = self.current_subscriptions
        batch_event = list()
        for channel in subscriptions:
            if channel:
                batch_event.append({"event": "addChannel", "channel": channel})
        self._send(batch_event)

    def inflate(self, data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

    def _on_message(self, message):
        super()._on_message(self.inflate(message))
