from Service.GetCrypto import get_xmr_eur_price
from asyncio import run

async def main():
    price = await get_xmr_eur_price()
    print(f"Prix XMR/EUR : {price}€")

# Exécution de la fonction asynchrone
run(main())