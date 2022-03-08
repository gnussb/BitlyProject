import asyncio
import math
from collections import defaultdict
from http import HTTPStatus
import aiohttp
from sanic.response import json
from sanic.exceptions import ServerError

from bitly.utils import flatten


class bitlyApi():

    def __init__(self, api_url, logger, api_key = None, unit = "day", units = 30, session = None, verify_ssl = True):
        """
        :param api_url: url for bitly API calls e.g. https://api-ssl.bitly.com/v4
        :param logger: inject logger to use for API
        :param api_key: YOUR_ACCESS_KEY https://app.bitly.com/settings/api/
            :param unit: The time interval to use to group the click counts
        :param units: The number of `unit` intervals to query for
        :param session: inject session object to use
        :param verify_ssl: check ssl certs (may need to be disabled when debugging)
        """
        self.api_url = api_url
        self._api_key = api_key
        self._log = logger
        self._session = session
        self._verify_ssl = verify_ssl
        self._unit = unit
        self._units = units

    def session(self, headers={}):
        """
        instantiates or retrieves the session object
        """
        if not self._session:
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def _group_id(self):
        """
        Retrieves the default group id for the user that owns the used access token
        """
        async with self.session().get(url=f"{self.api_url}/user", verify_ssl = self._verify_ssl) as response:
            if response.status == HTTPStatus.OK:
                return (await response.json())["default_group_guid"], True
            self._log.error(await response.text())
        return None, False

    async def _ids(self, group_id):
        """
        Retrieves a list of all of the bitlink ids that belong to a given group
        :param group_id: The group id whose bitlinks we want to retrieve
        """
        first_page = f"{self.api_url}/groups/{group_id}/bitlinks"
        number_of_concurrent_requests_to_make = 0

        responses = []

        # First request to get total number of links
        async with self.session().get(url=first_page, verify_ssl = self._verify_ssl) as response:
            if response.status == HTTPStatus.OK:
                resp_json = await response.json()
                responses.append(resp_json)
                pagination = resp_json["pagination"]
                total = pagination["total"]
                size = pagination["size"]
                number_of_concurrent_requests_to_make = int(math.ceil(total / size))
            else:
                self._log.error(await response.text())
                return None, False

        coroutines = (
            self.session().get(
                url=f"{self.api_url}/groups/{group_id}/bitlinks",
                params={"page": page_number},
                verify_ssl = self._verify_ssl,
            )
            for page_number in range(2, number_of_concurrent_requests_to_make + 1)
        )

        for response in await asyncio.gather(*coroutines):
            if response.status == HTTPStatus.OK:
                responses.append(await response.json())
            else:
                self._log.error(await response.text())
                return None, False

        links = flatten((response.get("links", {}) for response in responses))
        bitlinks = []
        for link in links:
            bitlinks.append(link["id"])
        return bitlinks, True

    async def _clicks_per_country(self, bitlinks):
        """
        Retrieves the number of bitlink clicks per country
        :param bitlinks: A collection of bitlink ids to use for the aggregation
        """
        params = {"unit": self._unit, "units": self._units}

        coroutines = (
            self.session().get(url=f"{self.api_url}/bitlinks/{bitlink}/countries", verify_ssl = self._verify_ssl, params=params)
            for bitlink in bitlinks
        )

        responses = []
        for response in await asyncio.gather(*coroutines):
            if response.status == HTTPStatus.OK:
                responses.append(await response.json())
            else:
                self._log.error(await response.text())
                return None, False

        metrics = flatten((response["metrics"] for response in responses))

        click_sums = defaultdict(int)
        for metric in metrics:
            click_sums[metric["value"]] += int(metric["clicks"])

        return click_sums, True

    async def get_bitlink_metrics_by_countries(self,req):
        """
        Calculates the average number of clicks on a bitlink, grouped by country, averaged
        over some period of time
        """
        headers = {"Authorization": req.token or f" Bearer {self._api_key}"}
        self._units = req.args.get('units') if req.args.get('units') and req.args.get('units').isdigit() and int(req.args.get('units')) in range(1,31) else self._units

        self.session(headers=headers)
        group_id, success = await self._group_id()
        if not success:
            raise ServerError(
                "There was a problem retrieving the group id",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        bitlinks, success = await self._ids(group_id)
        if not success:
            self._log.error("Problem getting bitlink ids")
            raise ServerError(
                "There was a problem getting the bitlink ids for your group",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        click_sums, success = await self._clicks_per_country(bitlinks)
        if not success:
            self._log.error("Problem getting bitlink metrics")
            raise ServerError(
                "There was a problem with retrieving metrics per country",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        metrics = {country: clicks / int(self._units) for country, clicks in click_sums.items()}
        metrics["type"] = "clicks"
        averaged = {"unit": self._unit, "units": self._units, "metrics": metrics}
        return json(averaged)