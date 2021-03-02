import requests
import random


class RandProxy:
    def __init__(self, types=None):
        self.token = ''
        self.url = ''
        self.proxy_list = self._proxy(types)

    def _header(self) -> dict:
        """
        Requests Header Set
        """
        return {
            'Authorization': 'Token ' + self.token,
        }

    def _proxy(self, types) -> dict or None:
        """
        API 로 모든 Proxy list 가져오기
        """
        if types:
            url = f'{self.url}?type={types}'
        else:
            url = self.url

        try:
            res = requests.get(url, headers=self._header(), timeout=5)
        except requests.RequestException:
            res = None
        else:
            res.raise_for_status()
            res = res.json()

        return res

    def get(self) -> dict:
        """
        전체 Proxy 중 랜덤으로 선택하여 리턴
        """
        secure_random = random.SystemRandom()
        proxy = secure_random.choice(self.proxy_list)

        return {
            'http': f'{proxy["protocol"]}://{proxy["user"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}'
        }
