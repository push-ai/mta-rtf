from typing import Dict, Iterable, Iterator, List, Optional

import datetime
import requests

import dlt
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2


# Reference for MTA Subway GTFS-realtime feeds
# See: https://api.mta.info/#/subwayRealTimeFeeds
FEED_URLS: Dict[str, str] = {
    # A C E + Rockaway Shuttle
    "ace": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    # B D F M + SF
    "bdfm": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    # G
    "g": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    # J Z
    "jz": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    # L
    "l": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    # N Q R W
    "nqrw": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    # 1 2 3 4 5 6 7 + S (Times Sq Shuttle)
    "main": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    # Staten Island Railway
    "sir": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}


def _download_feed(url: str, api_key: Optional[str] = None) -> gtfs_realtime_pb2.FeedMessage:
    headers = {"x-api-key": api_key} if api_key else None
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def _iter_trip_updates(feed_name: str, feed_msg: gtfs_realtime_pb2.FeedMessage) -> Iterator[Dict]:
    as_of = datetime.datetime.utcnow().isoformat()
    for entity in feed_msg.entity:
        if not entity.HasField("trip_update"):
            continue
        yield {
            "feed": feed_name,
            "entity_id": entity.id,
            "as_of": as_of,
            "trip_update": MessageToDict(entity.trip_update, preserving_proto_field_name=True),
        }


def _iter_vehicle_positions(feed_name: str, feed_msg: gtfs_realtime_pb2.FeedMessage) -> Iterator[Dict]:
    as_of = datetime.datetime.utcnow().isoformat()
    for entity in feed_msg.entity:
        if not entity.HasField("vehicle"):
            continue
        yield {
            "feed": feed_name,
            "entity_id": entity.id,
            "as_of": as_of,
            "vehicle": MessageToDict(entity.vehicle, preserving_proto_field_name=True),
        }


def _iter_alerts(feed_name: str, feed_msg: gtfs_realtime_pb2.FeedMessage) -> Iterator[Dict]:
    as_of = datetime.datetime.utcnow().isoformat()
    for entity in feed_msg.entity:
        if not entity.HasField("alert"):
            continue
        yield {
            "feed": feed_name,
            "entity_id": entity.id,
            "as_of": as_of,
            "alert": MessageToDict(entity.alert, preserving_proto_field_name=True),
        }


@dlt.source(name="mta_gtfs_realtime")
def mta_rt_source(
    api_key: Optional[str] = dlt.secrets.value,
    feeds: Optional[List[str]] = None,
) -> Iterable:
    """
    DLT source for MTA Subway GTFS-realtime feeds.

    Parameters
    - api_key: MTA API key (set via secrets). Required.
    - feeds: optional subset of feeds to pull, e.g. ["ace", "bdfm"]. Defaults to all.
    """
    selected = feeds or list(FEED_URLS.keys())

    @dlt.resource(name="trip_updates", write_disposition="append")
    def trip_updates() -> Iterator[Dict]:
        for feed_name in selected:
            feed_msg = _download_feed(FEED_URLS[feed_name], api_key)
            yield from _iter_trip_updates(feed_name, feed_msg)

    @dlt.resource(name="vehicle_positions", write_disposition="append")
    def vehicle_positions() -> Iterator[Dict]:
        for feed_name in selected:
            feed_msg = _download_feed(FEED_URLS[feed_name], api_key)
            yield from _iter_vehicle_positions(feed_name, feed_msg)

    @dlt.resource(name="alerts", write_disposition="append")
    def alerts() -> Iterator[Dict]:
        for feed_name in selected:
            feed_msg = _download_feed(FEED_URLS[feed_name], api_key)
            yield from _iter_alerts(feed_name, feed_msg)

    return [trip_updates, vehicle_positions, alerts]


