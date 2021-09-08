from typing import Type, TypeVar, MutableMapping, Any, Iterable, Generator

from datapipelines import DataSource, PipelineContext, Query, NotFoundError, validate_query

from .common import RiotAPIService, APINotFoundError
from ...data import Continent, Region, Platform, Queue, MatchType, QUEUE_IDS
from ...dto.match import MatchDto, MatchListDto, TimelineDto

T = TypeVar("T")


class MatchAPI(RiotAPIService):
    @DataSource.dispatch
    def get(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    _validate_get_match_query = Query. \
        has("id").as_(str).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform)

    @get.register(MatchDto)
    @validate_query(_validate_get_match_query)
    def get_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchDto:
        if "region" in query:
            query["continent"] = query["region"].continent
        if "platform" in query:
            query["continent"] = query["platform"].continent
        continent = Continent(query["continent"])
        id = query["id"]
        url = f"https://{continent.value}.api.riotgames.com/lol/match/v5/matches/{id}"
        try:
            app_limiter, method_limiter = self._get_rate_limiter(continent, "matches/id")
            data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
            # metadata = data["metadata"]
            data = data["info"]  # Drop the metadata
        except APINotFoundError as error:
            raise NotFoundError(str(error)) from error

        data["continent"] = continent.value
        for p in data["participants"]:
            puuid = p.get("puuid", None)
            if puuid is None:  # TODO: Figure out what bots are marked as in match-v5
                p["bot"] = True
            else:
                p["bot"] = False
        return MatchDto(data)

    _validate_get_many_match_query = Query. \
        has("ids").as_(Iterable).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform)

    @get_many.register(MatchDto)
    @validate_query(_validate_get_many_match_query)
    def get_many_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[MatchDto, None, None]:
        if "region" in query:
            query["continent"] = query["region"].continent
        if "platform" in query:
            query["continent"] = query["platform"].continent
        continent = query["continent"]

        def generator():
            for id in query["ids"]:
                url = f"https://{continent.value.lower()}.api.riotgames.com/lol/match/v4/matches/{id}"
                try:
                    app_limiter, method_limiter = self._get_rate_limiter(query["continent"], "matches/id")
                    data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
                except APINotFoundError as error:
                    raise NotFoundError(str(error)) from error

                for participant in data["participants"]:
                    participant.setdefault("runes", [])
                for p in data["participantIdentities"]:
                    aid = p.get("player", {}).get("currentAccountId", None)
                    if aid == 0:
                        p["player"]["bot"] = True

                data["gameId"] = id
                data["region"] = query["continent"].region.value
                yield MatchDto(data)

        return generator()

    _validate_get_match_list_query = Query. \
        has("puuid").as_(str).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform).also. \
        can_have("beginIndex").as_(int).also. \
        can_have("maxNumberOfMatches").as_(float).also. \
        can_have("queue").as_(Queue).also. \
        can_have("type").as_(MatchType)

    @get.register(MatchListDto)
    @validate_query(_validate_get_match_list_query)
    def get_match_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchListDto:
        if "region" in query:
            query["continent"] = query["region"].continent
        if "platform" in query:
            query["continent"] = query["platform"].continent

        params = {}
        riot_index_interval = 100
        params["start"] = query.get("beginIndex", 0)
        params["count"] = query.get("maxNumberOfMatches", None) or riot_index_interval

        queue = query.get("queue", None)
        if queue is not None:
            queue = QUEUE_IDS[queue]
            params["queue"] = QUEUE_IDS[queue]

        type_ = query.get("type", None)
        if type_ is not None:
            params["type"] = type_

        continent: Continent = query["continent"]
        puuid: str = query["puuid"]

        url = f"https://{continent.value.lower()}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        try:
            app_limiter, method_limiter = self._get_rate_limiter(continent, "matches/by-puuid/puuid")
            data = self._get(url, params, app_limiter=app_limiter, method_limiter=method_limiter)
        except APINotFoundError:
            data = []

        return MatchListDto({"match_ids": data, "continent": continent.value, "puuid": puuid, "type": type_, "queue": queue})

    _validate_get_many_match_list_query = Query. \
        has("puuid").as_(Iterable).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform).also. \
        can_have("beginIndex").as_(int).also. \
        can_have("maxNumberOfMatches").as_(int).also. \
        can_have("queues").as_(Iterable).also. \
        can_have("type").as_(Iterable)

    @get_many.register(MatchListDto)
    @validate_query(_validate_get_many_match_list_query)
    def get_many_match_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[MatchListDto, None, None]:
        raise NotImplementedError()

    _validate_get_timeline_query = Query. \
        has("id").as_(str).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform)

    @get.register(TimelineDto)
    @validate_query(_validate_get_timeline_query)
    def get_match_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> TimelineDto:
        if "region" in query:
            query["continent"] = query["region"].continent
        if "platform" in query:
            query["continent"] = query["platform"].continent
        continent = query["continent"]
        id = query["id"]
        url = f"https://{continent.value.lower()}.api.riotgames.com/lol/match/v4/timelines/by-match/{id}"
        try:
            app_limiter, method_limiter = self._get_rate_limiter(query["continent"], "timelines/by-match/id")
            data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
        except APINotFoundError as error:
            raise NotFoundError(str(error)) from error

        data["matchId"] = query["id"]
        data["continent"] = query["continent"].value
        return TimelineDto(data)

    _validate_get_many_timeline_query = Query. \
        has("ids").as_(Iterable).also. \
        has("continent").as_(Continent).or_("region").as_(Region).or_("platform").as_(Platform)

    @get_many.register(TimelineDto)
    @validate_query(_validate_get_many_timeline_query)
    def get_many_match_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[TimelineDto, None, None]:
        if "region" in query:
            query["continent"] = query["region"].continent
        if "platform" in query:
            query["continent"] = query["platform"].continent
        continent = query["continent"]

        def generator():
            for id in query["ids"]:
                url = f"https://{continent.value.lower()}.api.riotgames.com/lol/match/v4/timelines/by-match/{id}"
                try:
                    app_limiter, method_limiter = self._get_rate_limiter(query["continent"], "timelines/by-match/id")
                    data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
                except APINotFoundError as error:
                    raise NotFoundError(str(error)) from error

                data["matchId"] = id
                data["continent"] = query["continent"].value
                yield TimelineDto(data)

        return generator()
