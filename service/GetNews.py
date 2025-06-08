import aiohttp

async def get_xmr_stats():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://api.coingecko.com/api/v3/coins/monero') as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'mentions_twitter': data['community_data']['twitter_followers'],
                        'mentions_reddit': data['community_data']['reddit_subscribers'],
                        'facebook_likes': data['community_data']['facebook_likes'],
                        'sentiment_votes': data['sentiment_votes_up_percentage'],
                        'market_cap_rank': data['market_cap_rank']
                    }
                else:
                    print(f"Erreur API: {response.status}")
                    return None

    except Exception as e:
        print(f"Erreur : {str(e)}")
        return None
