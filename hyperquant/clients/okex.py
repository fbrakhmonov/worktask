import zlib
import re
from hyperquant.api import Platform, Interval
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
    is_source_in_timestring = True

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
            ParamName.AMOUNT,
            ParamName.SYMBOL  # added manually
        ],
    }

    def _generate_subscription(self, endpoint, symbol=None, **params):
        channel = super()._generate_subscription(endpoint, symbol, **params)
        return (channel, symbol)

    def parse(self, endpoint, data):
        # skip binary packets
        if "binary" in data:
            if isinstance(data["binary"], int) and data["binary"]:
                return
        if data:
            if data["channel"].endswith("_deals"):
                endpoint = Endpoint.TRADE
            elif data["channel"].endswith("_kline_Y"):
                endpoint = Endpoint.Candle
            if "data" in data and isinstance(data["data"], list):
                symbol = re.search("ok_sub_spot_(.+?)_deals", data["channel"])
                if symbol:
                    symbol = symbol.group(1)
                    data = data["data"]
                    data[0].append(symbol)
                else:
                    symbol = re.search("ok_sub_spot_(.+?)_kline_Y", data["channel"])
                    if symbol:
                        symbol = symbol.group(1)
                    data = data["data"]
                    data[0].append(symbol)
        # WARNING: FIXME: it gets nested list from api remove one of them
        return super().parse(endpoint, data)[0]


class OkexWSClient(WSClient):
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OkexWSConverterV1,
    }

    def _send_subscribe(self, subscriptions):
        batch_event = []
        for channel, symbol in subscriptions:
            if isinstance(channel, str) and channel == Endpoint.TRADE:
                batch_event.append(
                    {"event": "addChannel",
                        "channel": "ok_sub_spot_{}_deals".format(symbol)})
            elif isinstance(channel, str) and channel == Endpoint.CANDLE:
                batch_event.append(
                    {"event": "addChannel",
                        "channel": "ok_sub_spot_{}_kline_Y".format(symbol)})
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
