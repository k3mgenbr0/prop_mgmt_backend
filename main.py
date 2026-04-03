from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Any
import logging
import re

from fastapi import FastAPI, Depends, HTTPException, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator, ConfigDict
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, GoogleAPIError


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# App Initialization
# -----------------------------------------------------------------------------
app = FastAPI(
    title="Property Management API",
    version="1.2.0",
    description="FastAPI backend for Property Management App using BigQuery"
)

# -----------------------------------------------------------------------------
# CORS Configuration
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this in production
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PROJECT_ID = "mgmt545proj"
DATASET = "property_mgmt"

PROPERTIES_TABLE = f"`{PROJECT_ID}.{DATASET}.properties`"
INCOME_TABLE = f"`{PROJECT_ID}.{DATASET}.income`"
EXPENSES_TABLE = f"`{PROJECT_ID}.{DATASET}.expenses`"

STATE_REGEX = re.compile(r"^[A-Z]{2}$")
ZIP_REGEX = re.compile(r"^\d{5}(-\d{4})?$")
MONEY_PLACES = Decimal("0.01")


# -----------------------------------------------------------------------------
# Money Helpers
# -----------------------------------------------------------------------------
def normalize_money(value: Any) -> Decimal:
    try:
        if isinstance(value, Decimal):
            money = value
        else:
            money = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Money values must be valid numbers.")

    return money.quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)


def money_to_float(value: Any) -> float:
    return float(normalize_money(value))


def format_currency(value: Any) -> str:
    amount = normalize_money(value)

    if amount < 0:
        return f"(${abs(amount):,.2f})"
    return f"${amount:,.2f}"


def format_money_fields(record: dict, money_fields: List[str]) -> dict:
    output = dict(record)

    for field in money_fields:
        if field in output and output[field] is not None:
            output[field] = format_currency(output[field])

    return output


# -----------------------------------------------------------------------------
# Serialization Helpers
# -----------------------------------------------------------------------------
def serialize_row(row: dict) -> dict:
    serialized = {}

    for key, value in row.items():
        if isinstance(value, Decimal):
            serialized[key] = float(value)
        else:
            serialized[key] = value

    return serialized


# -----------------------------------------------------------------------------
# Dependency: BigQuery Client
# -----------------------------------------------------------------------------
def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# -----------------------------------------------------------------------------
# Exception Handlers
# -----------------------------------------------------------------------------
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail

    if isinstance(detail, dict):
        return JSONResponse(status_code=exc.status_code, content=detail)

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "message": str(detail)
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    details = []

    for err in exc.errors():
        field_path = " -> ".join(str(x) for x in err["loc"] if x != "body")
        details.append({
            "field": field_path,
            "message": err["msg"]
        })

    return JSONResponse(
        status_code=422,
        content={
            "error": True,
            "message": "The request could not be processed because some fields were invalid.",
            "details": details
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled server error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "message": "Something went wrong on our side. Please try again later."
        }
    )


# -----------------------------------------------------------------------------
# Base Model
# -----------------------------------------------------------------------------
class APIModel(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True
    )


# -----------------------------------------------------------------------------
# Pydantic Models
# -----------------------------------------------------------------------------
class PropertyBase(APIModel):
    name: str = Field(..., min_length=1, max_length=100, description="Property name")
    address: str = Field(..., min_length=1, max_length=200, description="Street address")
    city: str = Field(..., min_length=1, max_length=100)
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    postal_code: str = Field(..., min_length=5, max_length=10)
    property_type: str = Field(..., min_length=1, max_length=50)
    tenant_name: Optional[str] = Field(None, max_length=100)
    monthly_rent: Decimal = Field(..., ge=0, description="Monthly rent must be zero or greater")

    @field_validator("name", "address", "city", "property_type")
    @classmethod
    def required_text_cannot_be_blank(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("This field cannot be blank.")
        return cleaned

    @field_validator("state")
    @classmethod
    def validate_state(cls, value: str) -> str:
        cleaned = value.strip().upper()
        if not STATE_REGEX.fullmatch(cleaned):
            raise ValueError("State must be a 2-letter abbreviation like IN.")
        return cleaned

    @field_validator("postal_code")
    @classmethod
    def validate_postal_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not ZIP_REGEX.fullmatch(cleaned):
            raise ValueError("Postal code must be in 12345 or 12345-6789 format.")
        return cleaned

    @field_validator("tenant_name")
    @classmethod
    def clean_optional_tenant_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @field_validator("monthly_rent", mode="before")
    @classmethod
    def validate_monthly_rent(cls, value: Any) -> Decimal:
        money = normalize_money(value)
        if money < 0:
            raise ValueError("Monthly rent must be zero or greater.")
        return money


class PropertyCreate(PropertyBase):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Maple Grove Apartments",
                "address": "123 Main Street",
                "city": "Indianapolis",
                "state": "IN",
                "postal_code": "46204",
                "property_type": "Apartment",
                "tenant_name": "John Smith",
                "monthly_rent": 1450.00
            }
        }
    )


class PropertyUpdate(PropertyBase):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Maple Grove Apartments",
                "address": "123 Main Street",
                "city": "Indianapolis",
                "state": "IN",
                "postal_code": "46204",
                "property_type": "Apartment",
                "tenant_name": "Jane Doe",
                "monthly_rent": 1500.00
            }
        }
    )


class PropertyOut(APIModel):
    property_id: int
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str]
    monthly_rent: str


class IncomeCreate(APIModel):
    amount: Decimal = Field(..., gt=0, description="Income amount must be greater than 0")
    date: date
    description: Optional[str] = Field(None, max_length=250)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "amount": 1450.00,
                "date": "2026-04-03",
                "description": "April rent payment"
            }
        }
    )

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, value: Any) -> Decimal:
        money = normalize_money(value)
        if money <= 0:
            raise ValueError("Income amount must be greater than 0.")
        return money

    @field_validator("description")
    @classmethod
    def clean_description(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class IncomeOut(APIModel):
    income_id: int
    property_id: int
    amount: str
    date: date
    description: Optional[str]


class ExpenseCreate(APIModel):
    amount: Decimal = Field(..., gt=0, description="Expense amount must be greater than 0")
    date: date
    category: str = Field(..., min_length=1, max_length=100)
    vendor: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=250)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "amount": 250.75,
                "date": "2026-04-03",
                "category": "Maintenance",
                "vendor": "Ace Hardware",
                "description": "Plumbing repair supplies"
            }
        }
    )

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, value: Any) -> Decimal:
        money = normalize_money(value)
        if money <= 0:
            raise ValueError("Expense amount must be greater than 0.")
        return money

    @field_validator("category")
    @classmethod
    def validate_category(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Category cannot be blank.")
        return cleaned

    @field_validator("vendor", "description")
    @classmethod
    def clean_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class ExpenseOut(APIModel):
    expense_id: int
    property_id: int
    amount: str
    date: date
    category: str
    vendor: Optional[str]
    description: Optional[str]


class PropertyTotalsOut(APIModel):
    property_id: int
    total_income: str
    total_expenses: str
    net_cash_flow: str


class PropertySummaryOut(APIModel):
    property: PropertyOut
    totals: PropertyTotalsOut


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def run_query(
    bq: bigquery.Client,
    query: str,
    params: Optional[List[Any]] = None
):
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])

    try:
        return bq.query(query, job_config=job_config).result()

    except HTTPException:
        raise

    except BadRequest as exc:
        logger.exception("BigQuery bad request: %s", exc)
        raise HTTPException(
            status_code=400,
            detail={
                "error": True,
                "message": "The database rejected the request. Please check your inputs and try again."
            }
        )

    except GoogleAPIError as exc:
        logger.exception("BigQuery API error: %s", exc)
        raise HTTPException(
            status_code=503,
            detail={
                "error": True,
                "message": "The database service is temporarily unavailable. Please try again shortly."
            }
        )

    except Exception as exc:
        logger.exception("Unexpected database error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail={
                "error": True,
                "message": "We could not complete the database request right now. Please try again."
            }
        )


def fetch_one(
    bq: bigquery.Client,
    query: str,
    params: Optional[List[Any]] = None
):
    rows = list(run_query(bq, query, params))
    if not rows:
        return None
    return serialize_row(dict(rows[0]))


def fetch_all(
    bq: bigquery.Client,
    query: str,
    params: Optional[List[Any]] = None
) -> List[dict]:
    rows = run_query(bq, query, params)
    return [serialize_row(dict(row)) for row in rows]


def record_exists(bq: bigquery.Client, table: str, id_field: str, id_value: int) -> bool:
    query = f"""
        SELECT 1
        FROM {table}
        WHERE {id_field} = @id_value
        LIMIT 1
    """
    row = fetch_one(
        bq,
        query,
        [bigquery.ScalarQueryParameter("id_value", "INT64", id_value)]
    )
    return row is not None


def get_next_id(bq: bigquery.Client, table: str, id_field: str) -> int:
    query = f"""
        SELECT COALESCE(MAX({id_field}), 0) + 1 AS next_id
        FROM {table}
    """
    row = fetch_one(bq, query)

    if not row or "next_id" not in row:
        raise HTTPException(
            status_code=500,
            detail={
                "error": True,
                "message": "We could not generate a new record ID right now. Please try again."
            }
        )

    return int(row["next_id"])


def ensure_property_exists(bq: bigquery.Client, property_id: int):
    if not record_exists(bq, PROPERTIES_TABLE, "property_id", property_id):
        raise HTTPException(
            status_code=404,
            detail={
                "error": True,
                "message": f"No property was found with ID {property_id}."
            }
        )


def get_property_row(bq: bigquery.Client, property_id: int):
    query = f"""
        SELECT *
        FROM {PROPERTIES_TABLE}
        WHERE property_id = @property_id
    """
    row = fetch_one(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    if not row:
        raise HTTPException(
            status_code=404,
            detail={
                "error": True,
                "message": f"No property was found with ID {property_id}."
            }
        )

    return format_money_fields(row, ["monthly_rent"])


def get_property_totals_row(bq: bigquery.Client, property_id: int):
    ensure_property_exists(bq, property_id)

    income_query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_income
        FROM {INCOME_TABLE}
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_expenses
        FROM {EXPENSES_TABLE}
        WHERE property_id = @property_id
    """

    params = [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]

    income_row = fetch_one(bq, income_query, params) or {"total_income": 0}
    expense_row = fetch_one(bq, expense_query, params) or {"total_expenses": 0}

    total_income = money_to_float(income_row["total_income"])
    total_expenses = money_to_float(expense_row["total_expenses"])
    net_cash_flow = total_income - total_expenses

    totals = {
        "property_id": property_id,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_cash_flow": net_cash_flow
    }

    return format_money_fields(
        totals,
        ["total_income", "total_expenses", "net_cash_flow"]
    )


def shape_income_record(record: dict) -> dict:
    return format_money_fields(record, ["amount"])


def shape_expense_record(record: dict) -> dict:
    return format_money_fields(record, ["amount"])


# -----------------------------------------------------------------------------
# Additional Endpoint 1: Health Check
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "Property Management API is running successfully.",
        "version": app.version,
        "storage": "BigQuery",
        "deployment": "Google Cloud Run",
        "docs_url": "/docs",
        "cors_enabled": True,
        "json_responses": True
    }


# -----------------------------------------------------------------------------
# Required Endpoints: Properties
# -----------------------------------------------------------------------------
@app.get("/properties", response_model=List[PropertyOut])
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM {PROPERTIES_TABLE}
        ORDER BY property_id
    """
    results = fetch_all(bq, query)
    return [format_money_fields(row, ["monthly_rent"]) for row in results]


@app.get("/properties/{property_id}", response_model=PropertyOut)
def get_property(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    return get_property_row(bq, property_id)


# -----------------------------------------------------------------------------
# Additional Endpoint 2: Create Property
# -----------------------------------------------------------------------------
@app.post("/properties", response_model=PropertyOut, status_code=201)
def create_property(payload: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    property_id = get_next_id(bq, PROPERTIES_TABLE, "property_id")

    query = f"""
        INSERT INTO {PROPERTIES_TABLE}
        (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES
        (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
    """

    params = [
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("name", "STRING", payload.name),
        bigquery.ScalarQueryParameter("address", "STRING", payload.address),
        bigquery.ScalarQueryParameter("city", "STRING", payload.city),
        bigquery.ScalarQueryParameter("state", "STRING", payload.state),
        bigquery.ScalarQueryParameter("postal_code", "STRING", payload.postal_code),
        bigquery.ScalarQueryParameter("property_type", "STRING", payload.property_type),
        bigquery.ScalarQueryParameter("tenant_name", "STRING", payload.tenant_name),
        bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", money_to_float(payload.monthly_rent)),
    ]

    run_query(bq, query, params)

    return {
        "property_id": property_id,
        "name": payload.name,
        "address": payload.address,
        "city": payload.city,
        "state": payload.state,
        "postal_code": payload.postal_code,
        "property_type": payload.property_type,
        "tenant_name": payload.tenant_name,
        "monthly_rent": format_currency(payload.monthly_rent)
    }


# -----------------------------------------------------------------------------
# Additional Endpoint 3: Update Property
# -----------------------------------------------------------------------------
@app.put("/properties/{property_id}", response_model=PropertyOut)
def update_property(
    payload: PropertyUpdate,
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    query = f"""
        UPDATE {PROPERTIES_TABLE}
        SET
            name = @name,
            address = @address,
            city = @city,
            state = @state,
            postal_code = @postal_code,
            property_type = @property_type,
            tenant_name = @tenant_name,
            monthly_rent = @monthly_rent
        WHERE property_id = @property_id
    """

    params = [
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("name", "STRING", payload.name),
        bigquery.ScalarQueryParameter("address", "STRING", payload.address),
        bigquery.ScalarQueryParameter("city", "STRING", payload.city),
        bigquery.ScalarQueryParameter("state", "STRING", payload.state),
        bigquery.ScalarQueryParameter("postal_code", "STRING", payload.postal_code),
        bigquery.ScalarQueryParameter("property_type", "STRING", payload.property_type),
        bigquery.ScalarQueryParameter("tenant_name", "STRING", payload.tenant_name),
        bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", money_to_float(payload.monthly_rent)),
    ]

    run_query(bq, query, params)

    return {
        "property_id": property_id,
        "name": payload.name,
        "address": payload.address,
        "city": payload.city,
        "state": payload.state,
        "postal_code": payload.postal_code,
        "property_type": payload.property_type,
        "tenant_name": payload.tenant_name,
        "monthly_rent": format_currency(payload.monthly_rent)
    }


# -----------------------------------------------------------------------------
# Additional Endpoint 4: Delete Property
# -----------------------------------------------------------------------------
@app.delete("/properties/{property_id}")
def delete_property(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    delete_income_query = f"""
        DELETE FROM {INCOME_TABLE}
        WHERE property_id = @property_id
    """

    delete_expenses_query = f"""
        DELETE FROM {EXPENSES_TABLE}
        WHERE property_id = @property_id
    """

    delete_property_query = f"""
        DELETE FROM {PROPERTIES_TABLE}
        WHERE property_id = @property_id
    """

    params = [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]

    run_query(bq, delete_income_query, params)
    run_query(bq, delete_expenses_query, params)
    run_query(bq, delete_property_query, params)

    return {
        "message": f"Property {property_id} and its related records were deleted successfully."
    }


# -----------------------------------------------------------------------------
# Required Endpoints: Income
# -----------------------------------------------------------------------------
@app.get("/income/{property_id}", response_model=List[IncomeOut])
def get_income(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    query = f"""
        SELECT *
        FROM {INCOME_TABLE}
        WHERE property_id = @property_id
        ORDER BY date DESC, income_id DESC
    """

    results = fetch_all(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [shape_income_record(row) for row in results]


@app.post("/income/{property_id}", response_model=IncomeOut, status_code=201)
def create_income(
    payload: IncomeCreate,
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    income_id = get_next_id(bq, INCOME_TABLE, "income_id")

    query = f"""
        INSERT INTO {INCOME_TABLE}
        (income_id, property_id, amount, date, description)
        VALUES (@income_id, @property_id, @amount, @date, @description)
    """

    params = [
        bigquery.ScalarQueryParameter("income_id", "INT64", income_id),
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("amount", "FLOAT64", money_to_float(payload.amount)),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {
        "income_id": income_id,
        "property_id": property_id,
        "amount": format_currency(payload.amount),
        "date": payload.date,
        "description": payload.description
    }


# -----------------------------------------------------------------------------
# Required Endpoints: Expenses
# -----------------------------------------------------------------------------
@app.get("/expenses/{property_id}", response_model=List[ExpenseOut])
def get_expenses(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    query = f"""
        SELECT *
        FROM {EXPENSES_TABLE}
        WHERE property_id = @property_id
        ORDER BY date DESC, expense_id DESC
    """

    results = fetch_all(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [shape_expense_record(row) for row in results]


@app.post("/expenses/{property_id}", response_model=ExpenseOut, status_code=201)
def create_expense(
    payload: ExpenseCreate,
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    ensure_property_exists(bq, property_id)

    expense_id = get_next_id(bq, EXPENSES_TABLE, "expense_id")

    query = f"""
        INSERT INTO {EXPENSES_TABLE}
        (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
    """

    params = [
        bigquery.ScalarQueryParameter("expense_id", "INT64", expense_id),
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("amount", "FLOAT64", money_to_float(payload.amount)),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("category", "STRING", payload.category),
        bigquery.ScalarQueryParameter("vendor", "STRING", payload.vendor),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {
        "expense_id": expense_id,
        "property_id": property_id,
        "amount": format_currency(payload.amount),
        "date": payload.date,
        "category": payload.category,
        "vendor": payload.vendor,
        "description": payload.description
    }


# -----------------------------------------------------------------------------
# Additional Endpoint 5: Totals
# -----------------------------------------------------------------------------
@app.get("/totals/{property_id}", response_model=PropertyTotalsOut)
def get_totals(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    return get_property_totals_row(bq, property_id)


# -----------------------------------------------------------------------------
# Additional Endpoint 6: Property Summary
# -----------------------------------------------------------------------------
@app.get("/properties/{property_id}/summary", response_model=PropertySummaryOut)
def get_property_summary(
    property_id: int = Path(..., gt=0, description="Property ID must be a positive integer."),
    bq: bigquery.Client = Depends(get_bq_client)
):
    property_row = get_property_row(bq, property_id)
    totals_row = get_property_totals_row(bq, property_id)

    return {
        "property": property_row,
        "totals": totals_row
    }