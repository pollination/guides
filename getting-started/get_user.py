import os
import httpx
from pprint import pprint as print


class Const:
    api_key = os.environ['POLLINATION_API_KEY']
    url = 'https://api.pollination.cloud'

    class Endpoints:
        user = '/user'


headers = {
    'x-pollination-token': Const.api_key
}

client = httpx.Client(headers=headers, base_url=Const.url)

res = client.get(Const.Endpoints.user)

print(res.json())
