from datetime import date
from typing import Optional, List

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google.cloud import bigquery


# -----------------------------------------------------------------------------
# App Initialization
# -----------------------------------------------------------------------------
# FastAPI automatically gives you Swagger docs at /docs
app = FastAPI(
    title="Property Management API",
    version="1.0.0",
    description="FastAPI backend for Property Management App using BigQuery"
)

# -----------------------------------------------------------------------------
# CORS Configuration
# -----------------------------------------------------------------------------
# Allows frontend apps (React, etc.) to call your API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config (move to env vars later for production)
# -----------------------------------------------------------------------------
PROJECT_ID = "mgmt545proj"
DATASET = "property_mgmt"

# Fully-qualified BigQuery table names
PROPERTIES_TABLE = f"`{PROJECT_ID}.{DATASET}.properties`"
INCOME_TABLE = f"`{PROJECT_ID}.{DATASET}.income`"
EXPENSES_TABLE = f"`{PROJECT_ID}.{DATASET}.expenses`"


# -----------------------------------------------------------------------------
# Dependency: BigQuery Client
# -----------------------------------------------------------------------------
# This creates a client per request and safely closes it after
def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# -----------------------------------------------------------------------------
# Pydantic Models (Request + Response Validation)
# -----------------------------------------------------------------------------
# These ensure clean, validated input and consistent output

class PropertyBase(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


class PropertyCreate(PropertyBase):
    pass  # same fields as base


class PropertyUpdate(PropertyBase):
    pass  # full update (PUT)


class PropertyOut(PropertyBase):
    property_id: int  # returned to client


class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: Optional[str] = None


class IncomeOut(IncomeCreate):
    income_id: int
    property_id: int


class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None


class ExpenseOut(ExpenseCreate):
    expense_id: int
    property_id: int


class PropertyTotalsOut(BaseModel):
    property_id: int
    total_income: float
    total_expenses: float
    net_cash_flow: float


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

# Runs a BigQuery query with optional parameters
def run_query(bq: bigquery.Client, query: str, params=None):
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    try:
        return bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )


# Returns a single row or None
def fetch_one(bq, query, params=None):
    rows = list(run_query(bq, query, params))
    return dict(rows[0]) if rows else None


# Checks if a record exists (used for validation)
def record_exists(bq, table, id_field, id_value):
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


# Simple ID generation using MAX + 1 (fine for this assignment)
def get_next_id(bq, table, id_field):
    query = f"""
        SELECT COALESCE(MAX({id_field}), 0) + 1 AS next_id
        FROM {table}
    """
    row = fetch_one(bq, query)
    return int(row["next_id"])


# Ensures property exists before operating on it
def ensure_property_exists(bq, property_id):
    if not record_exists(bq, PROPERTIES_TABLE, "property_id", property_id):
        raise HTTPException(404, f"Property {property_id} not found")


# -----------------------------------------------------------------------------
# Health Check
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "API is running"}


# -----------------------------------------------------------------------------
# Properties Endpoints
# -----------------------------------------------------------------------------

# GET all properties
@app.get("/properties", response_model=List[PropertyOut])
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM {PROPERTIES_TABLE}
        ORDER BY property_id
    """
    results = run_query(bq, query)
    return [dict(row) for row in results]


# GET single property
@app.get("/properties/{property_id}", response_model=PropertyOut)
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
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
        raise HTTPException(404, "Property not found")

    return row


# CREATE property
@app.post("/properties", response_model=PropertyOut, status_code=201)
def create_property(payload: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    property_id = get_next_id(bq, PROPERTIES_TABLE, "property_id")

    query = f"""
        INSERT INTO {PROPERTIES_TABLE}
        VALUES (
            @property_id, @name, @address, @city, @state,
            @postal_code, @property_type, @tenant_name, @monthly_rent
        )
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
        bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", payload.monthly_rent),
    ]

    run_query(bq, query, params)

    return {"property_id": property_id, **payload.model_dump()}


# -----------------------------------------------------------------------------
# Income Endpoints
# -----------------------------------------------------------------------------

# GET income for a property
@app.get("/properties/{property_id}/income", response_model=List[IncomeOut])
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    ensure_property_exists(bq, property_id)

    query = f"""
        SELECT *
        FROM {INCOME_TABLE}
        WHERE property_id = @property_id
    """

    results = run_query(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [dict(row) for row in results]


# CREATE income record
@app.post("/properties/{property_id}/income", response_model=IncomeOut, status_code=201)
def create_income(property_id: int, payload: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    ensure_property_exists(bq, property_id)

    income_id = get_next_id(bq, INCOME_TABLE, "income_id")

    query = f"""
        INSERT INTO {INCOME_TABLE}
        VALUES (@income_id, @property_id, @amount, @date, @description)
    """

    params = [
        bigquery.ScalarQueryParameter("income_id", "INT64", income_id),
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("amount", "FLOAT64", payload.amount),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {"income_id": income_id, "property_id": property_id, **payload.model_dump()}


# -----------------------------------------------------------------------------
# Expenses Endpoints
# -----------------------------------------------------------------------------

# GET expenses for a property
@app.get("/properties/{property_id}/expenses", response_model=List[ExpenseOut])
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    ensure_property_exists(bq, property_id)

    query = f"""
        SELECT *
        FROM {EXPENSES_TABLE}
        WHERE property_id = @property_id
    """

    results = run_query(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [dict(row) for row in results]


# CREATE expense record
@app.post("/properties/{property_id}/expenses", response_model=ExpenseOut, status_code=201)
def create_expense(property_id: int, payload: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    ensure_property_exists(bq, property_id)

    expense_id = get_next_id(bq, EXPENSES_TABLE, "expense_id")

    query = f"""
        INSERT INTO {EXPENSES_TABLE}
        VALUES (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
    """

    params = [
        bigquery.ScalarQueryParameter("expense_id", "INT64", expense_id),
        bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
        bigquery.ScalarQueryParameter("amount", "FLOAT64", payload.amount),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("category", "STRING", payload.category),
        bigquery.ScalarQueryParameter("vendor", "STRING", payload.vendor),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {"expense_id": expense_id, "property_id": property_id, **payload.model_dump()}


# -----------------------------------------------------------------------------
# Totals Endpoint
# -----------------------------------------------------------------------------

# Returns aggregated financials for a property
@app.get("/properties/{property_id}/totals", response_model=PropertyTotalsOut)
def get_totals(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    ensure_property_exists(bq, property_id)

    query = f"""
        SELECT
            @property_id AS property_id,
            COALESCE(SUM(i.amount), 0) AS total_income,
            COALESCE(SUM(e.amount), 0) AS total_expenses,
            COALESCE(SUM(i.amount), 0) - COALESCE(SUM(e.amount), 0) AS net_cash_flow
        FROM {INCOME_TABLE} i
        FULL OUTER JOIN {EXPENSES_TABLE} e
        ON i.property_id = e.property_id
        WHERE i.property_id = @property_id OR e.property_id = @property_id
    """

    row = fetch_one(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return row