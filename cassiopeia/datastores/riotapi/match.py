from time import time
from typing import Type, TypeVar, MutableMapping, Any, Iterable, Generator

from datapipelines import DataSource, PipelineContext, Query, NotFoundError, validate_query

from .common import RiotAPIService, APINotFoundError
from ...data import Platform, Continent, Queue, MatchType, QUEUE_IDS
from ...dto.match import MatchDto, MatchListDto, TimelineDto
from ..uniquekeys import convert_region_to_platform

T = TypeVar("T")


class MatchAPI(RiotAPIService):
    @DataSource.dispatch
    def get(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    _validate_get_match_query = Query. \
        has("id").as_(int).also. \
        has("platform").as_(Platform)

    @get.register(MatchDto)
    @validate_query(_validate_get_match_query, convert_region_to_platform)
    def get_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchDto:
        continent = Continent(query["continent"])
        id = query["id"]
        url = f"https://{continent}.api.riotgames.com/lol/match/v5/matches/{id}"
        try:
            app_limiter, method_limiter = self._get_rate_limiter(continent.value, "matches/id")
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
        has("platform").as_(Platform)

    @get_many.register(MatchDto)
    @validate_query(_validate_get_many_match_query, convert_region_to_platform)
    def get_many_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[MatchDto, None, None]:
        def generator():
            for id in query["ids"]:
                url = "https://{platform}.api.riotgames.com/lol/match/v4/matches/{id}".format(platform=query["platform"].value.lower(), id=id)
                try:
                    app_limiter, method_limiter = self._get_rate_limiter(query["platform"], "matches/id")
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
                data["region"] = query["platform"].region.value
                yield MatchDto(data)

        return generator()

    _validate_get_match_list_query = Query. \
        has("puuid").as_(str).also. \
        has("continent").as_(Continent).also. \
        has("beginIndex").as_(int).also. \
        has("maxNumberOfMatches").as_(float).also. \
        can_have("queue").as_(Iterable).also. \
        has("types").as_(Iterable)

    @get.register(MatchListDto)
    @validate_query(_validate_get_match_list_query, convert_region_to_platform)
    def get_match_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchListDto:
        params = {}
        params["puuid"] = query["puuid"]

        riot_index_interval = 100
        params["start"] = query["beginIndex"]
        params["count"] = query["maxNumberOfMatches"] or riot_index_interval

        if "queue" in query:
            queue = Queue(query["queues"])
            params["queue"] = QUEUE_IDS[queue]
        else:
            queue = set()
        params["queue"] = queue

        if "type" in query:
            type_ = MatchType(query["type"])
        else:
            type_ = set()
        params["type"] = type_

        continent: Continent = query["continent"]
        puuid: str = params["puuid"]
        start: int = params["start"]
        count: int = params["count"]

        url = f"https://{continent}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type={type_.value.lower()}&start={start}&count={count}"
        if queue:
            url += f"&queue={queue.value.lower()}"
        try:
            app_limiter, method_limiter = self._get_rate_limiter(continent.value, "matches/by-puuid/puuid")
            data = self._get(url, params, app_limiter=app_limiter, method_limiter=method_limiter)
        except APINotFoundError:
            data = []

        for i, match_id in enumerate(data):
            data[i] = {
                "id": match_id,
                "continent": continent.value
            }
        return MatchListDto(data)

    _validate_get_many_match_list_query = Query. \
        has("puuid").as_(Iterable).also. \
        has("continent").as_(Continent).also. \
        can_have("beginIndex").as_(int).also. \
        can_have("maxNumberOfMatches").as_(int).also. \
        can_have("queues").as_(Iterable).also. \
        can_have("types").as_(Iterable)

    @get_many.register(MatchListDto)
    @validate_query(_validate_get_many_match_list_query, convert_region_to_platform)
    def get_many_match_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[MatchListDto, None, None]:
        raise NotImplementedError()

    _validate_get_timeline_query = Query. \
        has("id").as_(int).also. \
        has("platform").as_(Platform)

    @get.register(TimelineDto)
    @validate_query(_validate_get_timeline_query, convert_region_to_platform)
    def get_match_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> TimelineDto:
        url = "https://{platform}.api.riotgames.com/lol/match/v4/timelines/by-match/{id}".format(platform=query["platform"].value.lower(), id=query["id"])
        try:
            app_limiter, method_limiter = self._get_rate_limiter(query["platform"], "timelines/by-match/id")
            data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
        except APINotFoundError as error:
            raise NotFoundError(str(error)) from error

        data["matchId"] = query["id"]
        data["region"] = query["platform"].region.value
        return TimelineDto(data)

    _validate_get_many_timeline_query = Query. \
        has("ids").as_(Iterable).also. \
        has("platform").as_(Platform)

    @get_many.register(TimelineDto)
    @validate_query(_validate_get_many_timeline_query, convert_region_to_platform)
    def get_many_match_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> Generator[TimelineDto, None, None]:
        def generator():
            for id in query["ids"]:
                url = "https://{platform}.api.riotgames.com/lol/match/v4/timelines/by-match/{id}".format(platform=query["platform"].value.lower(), id=id)
                try:
                    app_limiter, method_limiter = self._get_rate_limiter(query["platform"], "timelines/by-match/id")
                    data = self._get(url, {}, app_limiter=app_limiter, method_limiter=method_limiter)
                except APINotFoundError as error:
                    raise NotFoundError(str(error)) from error

                data["matchId"] = id
                data["region"] = query["platform"].region.value
                yield TimelineDto(data)

        return generator()
