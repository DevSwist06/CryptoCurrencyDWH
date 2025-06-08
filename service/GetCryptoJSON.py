import requests
import os
from datetime import datetime, timezone
import json

def save_monero_price_json():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "monero",
        "vs_currencies": "usd"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Ajout du timestamp UTC dans le JSON
        now_utc = datetime.now(timezone.utc)
        timestamp_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO format
        data['timestamp_utc'] = timestamp_str

        # Chemin vers le dossier cible relatif au dossier courant (service/)
        output_dir = os.path.join("..", "data", "crypto_raw")
        os.makedirs(output_dir, exist_ok=True)

        # Nom de fichier avec timestamp
        filename = f"monero_price_{now_utc.strftime('%Y%m%dT%H%M%S')}.json"
        filepath = os.path.join(output_dir, filename)

        # Écriture du JSON dans le fichier
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"JSON sauvegardé dans : {filepath}")
        return filepath

    except requests.RequestException as e:
        print(f"Erreur lors de la récupération du prix : {e}")
        return None

# Exécution
save_monero_price_json()
