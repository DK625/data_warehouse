"""Billing tap class."""

from __future__ import annotations

from functools import cached_property

from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th

from tap_billing.client import (
    PostgresConnector,
    BillingV4InvoiceStream,
    DestinationConnector,
)


class TapBilling(SQLTap):
    """Billing tap class."""

    name = "tap-billing"
    default_stream_class = BillingV4InvoiceStream
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_uri",
            th.StringType,
            required=True,
            secret=True,
            description="SQLAlchemy URI for the source database",
        ),
        th.Property(
            "destination_sqlalchemy_uri",
            th.StringType,
            required=True,
            secret=True,
            description="SQLAlchemy URI for the destination database",
        ),
        th.Property(
            "destination_table",
            th.StringType,
            required=True,
            description="Destination table name",
        ),
    ).to_dict()

    @cached_property
    def _connector(self) -> PostgresConnector:
        return PostgresConnector(config=dict(self.config))

    @cached_property
    def _destination_connector(self) -> DestinationConnector:
        return DestinationConnector(config=dict(self.config))

    def discover_streams(self) -> list[Stream]:
        return [
            BillingV4InvoiceStream(
                tap=self,
                connector=self._connector,
                destination_connector=self._destination_connector,
            ),
        ]


if __name__ == "__main__":
    TapBilling.cli()
