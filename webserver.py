"""
Bit.ly Backend Coding Challenge
"""
import logging

import sanic
from sanic_json_logging import setup_json_logging
from bitly.api import bitlyApi

def main():
    """
    Main entrypoint of this application
    """
    app = sanic.Sanic(name="bitly-webserver")
    log = logging.getLogger("bitly-api")
    api_url = "https://api-ssl.bitly.com/v4"
    setup_json_logging(app)
    _bitlyApi = bitlyApi(api_url = api_url, logger = log, verify_ssl=False, api_key="YOUR_API_KEY_HERE")

    @app.route(uri="/countries/metrics", version="v1")
    async def countries_metrics(request):
        return await _bitlyApi.get_bitlink_metrics_by_countries(request)

    app.run(host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
