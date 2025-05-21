import ccxt.pro
from asyncio import run

async def get_xmr_eur_price():
    try:
        exchange = ccxt.pro.kucoin()
        orderbook = await exchange.watch_order_book('XMR/USDT')
        ask_price = orderbook['asks'][0][0]
        bid_price = orderbook['bids'][0][0]
        current_price = (ask_price + bid_price) / 2
        await exchange.close()
        return current_price

    except Exception as e:
        print(f"Erreur : {str(e)}")
        if 'exchange' in locals():
            await exchange.close()
        return None
