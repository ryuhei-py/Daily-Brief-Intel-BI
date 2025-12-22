from __future__ import annotations

from datetime import time
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class RateLimit(BaseModel):
    requests_per_minute: int = Field(gt=0)


class SourceEntry(BaseModel):
    source_id: str
    kind: str
    enabled: bool
    content_level: str
    rate_limit: RateLimit
    compliance: dict[str, bool]


class SourcesConfig(BaseModel):
    sources: list[SourceEntry]


class WatchlistPolicy(BaseModel):
    limit_enabled: bool


class MatchingPolicy(BaseModel):
    match_order: str


class WatchEntity(BaseModel):
    name: str
    metadata: dict[str, str] = Field(default_factory=dict)


class WatchlistConfig(BaseModel):
    watchlist_policy: WatchlistPolicy
    matching_policy: MatchingPolicy
    watch_entities: list[WatchEntity] = Field(default_factory=list)


class GeoConfig(BaseModel):
    geo_rollups: dict[str, list[str]]

    @model_validator(mode="after")
    def validate_tokyo_rollup(self) -> "GeoConfig":
        metro = self.geo_rollups.get("tokyo_metro", [])
        required = {"Tokyo", "Kanagawa", "Chiba", "Saitama"}
        if not required.issubset(set(metro)):
            raise ValueError("geo_rollups.tokyo_metro must include Tokyo/Kanagawa/Chiba/Saitama")
        return self


class ScheduleConfig(BaseModel):
    daily_time_jst: time = Field(default=time(hour=7, minute=0))


class SeriesResolver(BaseModel):
    type: str
    value: Optional[str] = None


class SeriesEntry(BaseModel):
    key: str
    resolver: SeriesResolver


class SeriesConfig(BaseModel):
    series: list[SeriesEntry]


class ScoringRule(BaseModel):
    name: str
    weight: float = Field(default=1.0)


class ScoringConfig(BaseModel):
    default_score: float = Field(default=0.0)
    rules: list[ScoringRule] = Field(default_factory=list)


class AlertChannel(BaseModel):
    channel: str
    target: Optional[str] = None


class AlertsConfig(BaseModel):
    enabled: bool = Field(default=True)
    channels: list[AlertChannel] = Field(default_factory=list)


CONFIG_MODEL_MAP = {
    "sources.yml": SourcesConfig,
    "watchlist.yml": WatchlistConfig,
    "geo.yml": GeoConfig,
    "schedule.yml": ScheduleConfig,
    "series.yml": SeriesConfig,
    "scoring.yml": ScoringConfig,
    "alerts.yml": AlertsConfig,
}
