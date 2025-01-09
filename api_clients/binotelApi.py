import requests
import json

class BinotelApi:
    def __init__(self, key, secret, api_host=None, api_version='4.0', api_format='json'):
        self.key = key
        self.secret = secret
        self.api_host = api_host or 'https://api.binotel.com/api/'
        self.api_version = api_version
        self.api_format = api_format
        self.disable_ssl_checks = False
        self.debug = False

    def send_request(self, endpoint, params):
        # Добавляем ключ и секрет к параметрам
        params['key'] = self.key
        params['secret'] = self.secret

        # Преобразование параметров в JSON
        post_data = json.dumps(params)

        # Формируем полный URL
        url = f"{self.api_host}{self.api_version}/{endpoint}.{self.api_format}"

        if self.debug:
            print(f"[CLIENT] Send request: {post_data}")

        # Настраиваем запрос
        try:
            response = requests.post(
                url,
                data=post_data,
                headers={
                    'Content-Length': str(len(post_data)),
                    'Content-Type': 'application/json',
                },
                verify=not self.disable_ssl_checks
            )
            if self.debug:
                print(f"[CLIENT] Server response code: {response.status_code}")

            # Проверка успешности запроса
            if response.status_code != 200:
                if self.debug:
                    print(f"[CLIENT] Server error: {response.text}")
                return {"status": "error", "code": response.status_code, "message": response.text}

            # Декодируем JSON-ответ
            result = response.json()
            if self.debug:
                print(f"[CLIENT] Response JSON: {result}")

            return result

        except requests.RequestException as e:
            if self.debug:
                print(f"[CLIENT] Request error: {e}")
            return {"status": "error", "message": str(e)}

    def disable_ssl_checks(self):
        """Очень небезопасно: отключает проверку подлинности SSL-сертификатов."""
        self.disable_ssl_checks = True

