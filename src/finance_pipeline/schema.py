"""Single source of truth for the data contract. Production code imports from here."""

COLUMNS = (
    "transaction_id",
    "account_id",
    "transaction_ts",
    "transaction_type",
    "amount",
    "currency",
    "status",
    "source_updated_ts",
)

# Spark timestamp pattern matching the CSV format YYYY-MM-DDTHH:MM:SS (UTC).
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

VALID_TYPES = ("CREDIT", "DEBIT")
VALID_CURRENCIES = ("INR", "USD")
VALID_STATUSES = ("POSTED", "PENDING", "FAILED")
