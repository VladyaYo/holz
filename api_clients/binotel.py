import aiohttp
import time

class BinotelClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.binotel.com"

    async def send_request(self, method, params):
        timestamp = int(time.time())
        url = f"{self.base_url}/{method}?key={self.api_key}&timestamp={timestamp}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, ssl=False) as response:
                response.raise_for_status()
                return await response.json()