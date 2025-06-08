import tweepy

def get_word_impression_rate(word, api_key, api_key_secret, access_token, access_token_secret):
    try:
        # Authentification avec l'API Twitter
        auth = tweepy.OAuthHandler(api_key, api_key_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

        # Recherche de tweets contenant le mot
        tweets = api.search_tweets(q=word, count=100, lang="en")
        impression_rate = len(tweets)

        print(f"Taux d'impression pour '{word}': {impression_rate}")
        return impression_rate

    except Exception as e:
        print(f"Erreur : {str(e)}")
        return None