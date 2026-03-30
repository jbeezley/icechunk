"""Parse GRIB yaml.gz index files into a message catalog."""

import gzip
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class GribMessage:
    """A single GRIB message with its location and metadata."""
    short_name: str
    type_of_level: str
    level: int
    forecast_hour: int
    uri: str          # S3 path to the GRIB file
    offset: int       # byte offset within the file
    length: int       # byte length of the message
    ni: int           # number of longitude points
    nj: int           # number of latitude points


@dataclass
class GribCatalog:
    """All messages from a set of GRIB index files, grouped for zarr."""
    messages: list[GribMessage] = field(default_factory=list)

    def add_from_yaml(self, yaml_path: str | Path) -> None:
        """Parse a yaml.gz index file and add its messages."""
        with gzip.open(yaml_path, "rt") as f:
            data = yaml.safe_load(f)

        grib_uri = data["uri"]

        for msg in data["messages"]:
            computed = msg["computed"]
            section3 = msg["sections"][3]

            # Extract forecast hour from stepRange
            # stepRange can be "0", "6", "0-6", "5-6"
            step_str = computed["stepRange"]
            # Last number in the string is the forecast hour
            forecast_hour = int(step_str.split("-")[-1])

            self.messages.append(GribMessage(
                short_name=computed["shortName"],
                type_of_level=computed["typeOfLevel"],
                level=computed["level"],
                forecast_hour=forecast_hour,
                uri=grib_uri,
                offset=msg["offset"],
                length=msg["length"],
                ni=section3["Ni"],
                nj=section3["Nj"],
            ))

    def groups(self) -> dict[str, list[GribMessage]]:
        """Group messages by (shortName, typeOfLevel) -> array name."""
        result: dict[str, list[GribMessage]] = {}
        for msg in self.messages:
            key = f"{msg.short_name}_{msg.type_of_level}"
            result.setdefault(key, []).append(msg)
        return result
