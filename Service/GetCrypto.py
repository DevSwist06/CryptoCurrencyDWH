import time

import ccxt.pro
from asyncio import gather, run

global last_binance_price
global last_okx_price
global bid_prices
global ask_prices

bid_prices = {}
ask_prices = {}

async def symbol_loop(exchange, symbol):
    global bid_prices
    global ask_prices
    print("Starting the", exchange.id, "symbol loop with", symbol)
    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol)
            now = exchange.milliseconds()
            bid_prices[exchange.id] = orderbook["bids"][0][0]
            ask_prices[exchange.id] = orderbook["asks"][0][0]
            min_ask_ex = min(ask_prices, key=ask_prices.get)
            max_bid_ex = max(bid_prices, key=bid_prices.get)
            min_ask_price = ask_prices[min_ask_ex]
            max_bid_price = bid_prices[max_bid_ex]
            print(
                f"{exchange.iso8601(now)}: Buy XMR/USDT for {min_ask_price}$, Sell XMR/USDT for {max_bid_price}$"
            )
            time.sleep(2)
        except Exception as e:
            print(str(e))
            break  # you can break just this one loop if it fails

async def exchange_loop(exchange_id, symbols):
    print("Starting the", exchange_id, "exchange loop with", symbols)
    exchange = getattr(ccxt.pro, exchange_id)()
    loops = [symbol_loop(exchange, symbol) for symbol in symbols]
    await gather(*loops)
    await exchange.close()

async def main():
    exchanges = {
        #"okx": ["BTC/USDT"],
        "kucoin": ["XMR/USDT"],
        #"binance": ["BTC/USDT"],
        #"htx": ["BTC/USD"],
    }
    loops = [
        exchange_loop(exchange_id, symbols)
        for exchange_id, symbols in exchanges.items()
    ]
    await gather(*loops)

run(main())
