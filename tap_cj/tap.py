"""CJ tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_cj import streams


class TapCJ(Tap):
    """CJ tap class."""

    name = "tap-cj"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "publisher_ids",
            th.ArrayType(th.StringType),
            description="The publisher ID to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.CJStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CommissionsStream(self),
        ]


if __name__ == "__main__":
    TapCJ.cli()
