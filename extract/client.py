"""SQL client handling.

This includes BillingStream and BillingConnector.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Iterable, Optional, Type, Union, List

import pytz
import sqlalchemy
from singer_sdk import SQLConnector, Stream, Tap
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_FULL_TABLE
from sqlalchemy import select, func

logger = logging.getLogger(__name__)


def retry(tries, delay, logger):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    msg = f"{e.__class__.__name__}: {e}"
                    logger.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
            return func(*args, **kwargs)

        return wrapped

    return wrapper


class PostgresConnector(SQLConnector):
    """Connects to the Postgres SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return config["sqlalchemy_uri"]

    def get_connection(self):
        """Return a connection to the database."""
        return self._connect()

    @staticmethod
    def to_jsonschema_type(
        sql_type: Union[
            str,
            sqlalchemy.types.TypeEngine,
            Type[sqlalchemy.types.TypeEngine],
            sqlalchemy.dialects.postgresql.ARRAY,
            Any,
        ]
    ) -> dict:
        """Return a JSON Schema representation of the provided type.
        Overidden from SQLConnector to correctly handle JSONB and Arrays.
        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.
        Args
        ----
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.
        Raises
        ------
            ValueError: If the type received could not be translated to jsonschema.
        Returns
        -------
            The JSON Schema representation of the provided type.
        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__

        if type_name is not None and type_name == "JSONB":
            return th.ObjectType().type_dict

        if (
            type_name is not None
            and isinstance(sql_type, sqlalchemy.dialects.postgresql.ARRAY)
            and type_name == "ARRAY"
        ):
            array_type = PostgresConnector.sdk_typing_object(sql_type.item_type)
            return th.ArrayType(array_type).type_dict
        return PostgresConnector.sdk_typing_object(sql_type).type_dict

    @staticmethod
    def sdk_typing_object(
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType:
        """Return the JSON Schema dict that describes the sql type.
        Args
        ----
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.
        Raises
        ------
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.
        Returns
        -------
            A compatible JSON Schema type definition.
        """
        sqltype_lookup: dict[
            str,
            th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType,
        ] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking
            # precedence. If the SQL-provided type contains the type name on
            #  the left, the mapping will return the respective singer type.
            "timestamp": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "int": th.IntegerType(),
            "number": th.NumberType(),
            "decimal": th.NumberType(),
            "double": th.NumberType(),
            "float": th.NumberType(),
            "string": th.StringType(),
            "text": th.StringType(),
            "char": th.StringType(),
            "bool": th.BooleanType(),
            "variant": th.StringType(),
        }
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(from_type, sqlalchemy.types.TypeEngine):
            type_name = from_type.__name__
        else:
            raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str


class BillingConnector(PostgresConnector):
    pass


class DestinationConnector(PostgresConnector):
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._config = config

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return config["destination_sqlalchemy_uri"]

    def get_destination_table(self):
        return self.get_table(full_table_name=self._config["destination_table"])


class BillingV4InvoiceStream(Stream):
    name = "v4_invoices"
    primary_keys = ["billing_cycle", "account_id", "service_type", "invoice_id"]

    def __init__(
        self,
        tap: Tap,
        connector: PostgresConnector,
        destination_connector: DestinationConnector,
        schema: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(tap=tap, schema=schema, name=name)
        self._connector = connector
        self._destination_connector = destination_connector

    @staticmethod
    def _generate_id(row):
        return f"{row['billing_cycle']}|{row['account_id']}|{row['service_type']}|{row['invoice_id']}"

    @staticmethod
    def _extract_id(row_id):
        d = row_id.split("|")
        return {
            "billing_cycle": d[0],
            "account_id": d[1],
            "service_type": d[2],
            "invoice_id": d[3],
        }

    def _get_billing_cycles(self) -> List[str]:
        invoice_table = self._connector.get_table(
            full_table_name="invoices",
        )
        start_cycle = "01-02-2023"
        query = (
            select(
                [
                    invoice_table.c.billing_cycle,
                    func.to_char(
                        func.to_date(invoice_table.c.billing_cycle, "01-MM-YYYY"),
                        "YYYY-MM-DD",
                    ),
                ]
            )
            .where(
                invoice_table.c.billing_version.like("4%"),
                func.to_date(invoice_table.c.billing_cycle, "01-MM-YYYY") >= func.to_date(start_cycle, "DD-MM-YYYY"),
            )
            .group_by(invoice_table.c.billing_cycle)
        )
        with self._connector.get_connection() as conn:
            list_cycle = [row for row in conn.execute(query)]
        list_cycle.sort(key=lambda x: x[1], reverse=True)
        return [row[0] for row in list_cycle]

    def get_destination_deleted_records(self, billing_cycle: str, actual_ids: List[str]) -> List[str]:
        table = self._destination_connector.get_destination_table()
        query = (
            select(
                [
                    table.c.billing_cycle,
                    table.c.account_id,
                    table.c.service_type,
                    table.c.invoice_id,
                ]
            )
            .where(table.c.billing_cycle == billing_cycle)
            .group_by(
                table.c.billing_cycle,
                table.c.account_id,
                table.c.service_type,
                table.c.invoice_id,
            )
        )
        existing_ids = []
        with self._destination_connector.get_connection() as conn:
            for row in conn.execute(query):
                dict_row = dict(getattr(row, "_mapping"))
                existing_ids.append(self._generate_id(dict_row))
        deleted_rows = []
        set_actual_ids = set(actual_ids)
        for row_id in existing_ids:
            if row_id not in set_actual_ids:
                deleted_rows.append(row_id)
        return deleted_rows

    def get_records(self, context: dict | None) -> Iterable[dict | tuple[dict, dict]]:
        if context:
            raise NotImplementedError(f"Stream '{self.name}' does not support partitioning.")

        invoices_table = self._connector.get_table(
            full_table_name="invoices",
        )
        invoice_invoice_line_table = self._connector.get_table(
            full_table_name="invoice_invoice_line",
        )
        invoice_lines_table = self._connector.get_table(
            full_table_name="invoice_lines",
        )

        all_counter = 0
        for billing_cycle in self._get_billing_cycles():
            logger.info(f"Processing billing cycle: {billing_cycle}")
            query = (
                select(
                    [
                        invoices_table.c.billing_cycle,
                        invoices_table.c.account_id,
                        invoice_lines_table.c.product_name.label("service_type"),
                        invoices_table.c.id.label("invoice_id"),
                        invoices_table.c.status,
                        invoices_table.c.is_trial,
                        func.sum(invoice_lines_table.c.subtotal).label("subtotal"),
                        func.sum(invoice_lines_table.c.total).label("total"),
                    ]
                )
                .select_from(
                    invoice_lines_table.join(
                        invoice_invoice_line_table,
                        invoice_invoice_line_table.c.invoice_line_id == invoice_lines_table.c.id,
                    ).join(
                        invoices_table,
                        invoice_invoice_line_table.c.invoice_id == invoices_table.c.id,
                    )
                )
                .where(
                    invoices_table.c.billing_version.like("4%"),
                    invoices_table.c.billing_cycle == billing_cycle,
                )
                .group_by(
                    invoices_table.c.billing_cycle,
                    invoices_table.c.account_id,
                    invoice_lines_table.c.product_name,
                    invoices_table.c.id,
                )
            )
            counter = 0
            actual_ids = []
            with self._connector.get_connection() as conn:
                for record in conn.execute(query):
                    counter += 1
                    r = dict(getattr(record, "_mapping"))
                    required_fields = ["billing_cycle", "account_id", "service_type", "invoice_id"]
                    missing_fields = []
                    for field in required_fields:
                        if not r.get(field):
                            missing_fields.append(field)
                    if missing_fields:
                        logger.error(
                            "Record %s is missing required fields: %s",
                            r,
                            missing_fields,
                        )
                        continue
                    r["_sdc_deleted_at"] = None
                    actual_ids.append(self._generate_id(r))
                    yield r
            all_counter += counter
            logger.info("Processed %s records", counter)
            logger.info(f"Stream {self.name} processed {all_counter} records")
            logger.info("Start processing deleted records")
            deleted_rows = self.get_destination_deleted_records(billing_cycle, actual_ids)
            logger.info("Founded deleted rows: %s", len(deleted_rows))
            for row_id in deleted_rows:
                deleted_row = self._extract_id(row_id)
                deleted_row["_sdc_deleted_at"] = datetime.now(tz=pytz.utc).isoformat()
                logger.info("Deleted row: %s", deleted_row)
                yield deleted_row

    @property
    def replication_method(self) -> str:
        return REPLICATION_FULL_TABLE

    @property
    def schema(self) -> dict:
        return th.PropertiesList(
            th.Property("billing_cycle", th.StringType),
            th.Property("account_id", th.StringType),
            th.Property("service_type", th.StringType),
            th.Property("invoice_id", th.StringType),
            th.Property("status", th.StringType),
            th.Property("is_trial", th.BooleanType),
            th.Property("subtotal", th.NumberType),
            th.Property("total", th.NumberType),
            th.Property("_sdc_deleted_at", th.DateTimeType),
        ).to_dict()
