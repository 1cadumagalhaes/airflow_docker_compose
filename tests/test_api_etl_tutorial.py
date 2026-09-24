"""Unit tests for Launch Library response normalization."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / 'dags'
sys.path.insert(0, str(DAGS_DIR))

from lib.launch_api import LaunchResponse, normalize_launch  # noqa: E402


def test_normalize_launch_uses_pad_country_and_nullable_details():
    launch = {
        'id': 'launch-id',
        'name': 'Sputnik | Sputnik 1',
        'net': '1957-10-04T19:28:34Z',
        'status': {'name': 'Launch Successful'},
        'launch_service_provider': None,
        'rocket': None,
        'mission': None,
        'pad': {
            'country': {'alpha_2_code': 'KZ'},
            'location': None,
        },
    }

    parsed = LaunchResponse.model_validate({'results': [launch]}).results[0]

    assert normalize_launch(parsed) == {
        'id': 'launch-id',
        'name': 'Sputnik | Sputnik 1',
        'provider': None,
        'rocket': None,
        'orbit': None,
        'country': 'KZ',
        'status': 'Launch Successful',
        'launch_time': '1957-10-04T19:28:34Z',
    }


def test_normalize_launch_requires_identity_name_and_launch_time():
    with pytest.raises(ValidationError):
        LaunchResponse.model_validate({'results': [{'id': 'launch-id'}]})


def test_parse_launch_rejects_non_object():
    with pytest.raises(ValidationError):
        LaunchResponse.model_validate({'results': ['not a launch object']})
