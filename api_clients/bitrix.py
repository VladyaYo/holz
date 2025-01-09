import aiohttp

class BitrixClient:
    def __init__(self, domain, region, auth_key):
        self.base_url = f"https://{domain}.{region}/rest/28/{auth_key}"

    async def send_request(self, method, params):
        url = f"{self.base_url}/{method}.json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
