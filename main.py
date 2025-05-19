from Service.GetCrypto import get_xmr_eur_price
from Service.GetNews import get_xmr_stats
from asyncio import run

async def main():
    price = await get_xmr_eur_price()
    print(f"Prix XMR/EUR : {price}€")

# Exécution de la fonction asynchrone
run(main())

async def main():
    stats = await get_xmr_stats()
    if stats:
        print("Statistiques Monero :")
        for key, value in stats.items():
            print(f"{key}: {value}")

run(main())