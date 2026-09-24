"""Fetch Launch Library 2 data without Airflow.

Run this first-stage tutorial script with `uv run dags/launch_api.py`. The DAG imports
`fetch_launches` and supplies parameters, while Airflow handles scheduling,
retries, and downstream database tasks.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

LAUNCH_LIBRARY_URL = 'https://ll.thespacedevs.com/2.3.0/launches/'
log = logging.getLogger(__name__)


class LaunchLibraryModel(BaseModel):
    """Base for the subset of API fields this tutorial consumes."""

    model_config = ConfigDict(extra='ignore')


class NamedResource(LaunchLibraryModel):
    name: str | None = None


class Orbit(NamedResource):
    pass


class Mission(NamedResource):
    orbit: Orbit | None = None


class RocketConfiguration(LaunchLibraryModel):
    full_name: str | None = None


class Rocket(LaunchLibraryModel):
    configuration: RocketConfiguration | None = None


class Country(LaunchLibraryModel):
    alpha_2_code: str | None = None
    country_code: str | None = None


class LaunchLocation(LaunchLibraryModel):
    country: Country | None = None


class LaunchPad(LaunchLibraryModel):
    country: Country | None = None
    location: LaunchLocation | None = None


class LaunchRecord(LaunchLibraryModel):
    id: str
    name: str
    net: str
    status: NamedResource | None = None
    launch_service_provider: NamedResource | None = None
    rocket: Rocket | None = None
    mission: Mission | None = None
    pad: LaunchPad | None = None


class LaunchResponse(LaunchLibraryModel):
    results: list[LaunchRecord]


def normalize_launch(launch: LaunchRecord) -> dict[str, str | None]:
    """Map an API launch to the flat staging-table shape."""
    pad = launch.pad
    country = (pad.country if pad else None) or (
        pad.location.country if pad and pad.location else None
    )

    return {
        'id': launch.id,
        'name': (launch.mission.name if launch.mission else None) or launch.name,
        'provider': (
            launch.launch_service_provider.name
            if launch.launch_service_provider
            else None
        ),
        'rocket': (
            launch.rocket.configuration.full_name
            if launch.rocket and launch.rocket.configuration
            else None
        ),
        'orbit': (
            launch.mission.orbit.name
            if launch.mission and launch.mission.orbit
            else None
        ),
        'country': (
            (country.alpha_2_code or country.country_code) if country else None
        ),
        'status': launch.status.name if launch.status else None,
        'launch_time': launch.net,
    }


def fetch_launches(
    limit: int = 50, mode: str = 'detailed'
) -> list[dict[str, str | None]]:
    """Request launches, validate the API response, and return normalized rows."""
    import requests

    if not 1 <= limit <= 100:
        raise ValueError('limit must be between 1 and 100.')
    if mode not in {'list', 'normal', 'detailed'}:
        raise ValueError("mode must be 'list', 'normal', or 'detailed'.")

    log.info('Requesting launches (limit=%s, mode=%s)', limit, mode)
    response = requests.get(
        LAUNCH_LIBRARY_URL,
        params={'limit': limit, 'mode': mode},
        timeout=30,
    )
    response.raise_for_status()
    log.info('Launch Library responded with HTTP %s', response.status_code)

    payload: Any = response.json()
    try:
        parsed_response = LaunchResponse.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(
            'Launch Library returned an invalid response payload.'
        ) from exc

    log.info('Received %d launch records', len(parsed_response.results))
    rows = [normalize_launch(launch) for launch in parsed_response.results]
    log.info('Normalized %d launch records', len(rows))
    return rows


def main() -> None:
    """Run the standalone extraction example and print its result summary."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    launches = fetch_launches()
    print(f'Fetched {len(launches)} launches')
    for launch in launches:
        print(launch)


if __name__ == '__main__':
    main()
