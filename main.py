from datetime import date
from typing import Optional, List, Any

from fastapi import FastAPI, Depends, HTTPException, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator
from google.cloud import bigquery


# -----------------------------------------------------------------------------
# App Initialization
# -----------------------------------------------------------------------------
# FastAPI automatically provides Swagger docs at /docs
app = FastAPI(
    title="Property Management API",
    version="1.0.0",
    description="FastAPI backend for Property Management App using BigQuery"
)

# -----------------------------------------------------------------------------
# CORS Configuration
# -----------------------------------------------------------------------------
# Allows frontend apps to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this in production if needed
    allow_credentials=True,
    allow_methods=["*"],
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
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "message": "Something went wrong on our side. Please try again later."
        }
    )


# -----------------------------------------------------------------------------
# Pydantic Models
# -----------------------------------------------------------------------------
class PropertyBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Property name")
    address: str = Field(..., min_length=1, max_length=200, description="Street address")
    city: str = Field(..., min_length=1, max_length=100)
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    postal_code: str = Field(..., min_length=5, max_length=10)
    property_type: str = Field(..., min_length=1, max_length=50)
    tenant_name: Optional[str] = Field(None, max_length=100)
    monthly_rent: float = Field(..., ge=0, description="Monthly rent must be zero or greater")

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
        if len(cleaned) != 2:
            raise ValueError("State must be a 2-letter abbreviation like IN.")
        return cleaned

    @field_validator("postal_code")
    @classmethod
    def validate_postal_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Postal code cannot be blank.")
        return cleaned

    @field_validator("tenant_name")
    @classmethod
    def clean_optional_tenant_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class PropertyCreate(PropertyBase):
    pass


class PropertyUpdate(PropertyBase):
    pass


class PropertyOut(PropertyBase):
    property_id: int


class IncomeCreate(BaseModel):
    amount: float = Field(..., gt=0, description="Income amount must be greater than 0")
    date: date
    description: Optional[str] = Field(None, max_length=250)

    @field_validator("description")
    @classmethod
    def clean_description(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class IncomeOut(IncomeCreate):
    income_id: int
    property_id: int


class ExpenseCreate(BaseModel):
    amount: float = Field(..., gt=0, description="Expense amount must be greater than 0")
    date: date
    category: str = Field(..., min_length=1, max_length=100)
    vendor: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=250)

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


class ExpenseOut(ExpenseCreate):
    expense_id: int
    property_id: int


class PropertyTotalsOut(BaseModel):
    property_id: int
    total_income: float
    total_expenses: float
    net_cash_flow: float


class PropertySummaryOut(BaseModel):
    property: PropertyOut
    totals: PropertyTotalsOut


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def run_query(bq: bigquery.Client, query: str, params: Optional[List[Any]] = None):
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    try:
        return bq.query(query, job_config=job_config).result()
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={
                "error": True,
                "message": "We could not complete the database request right now. Please try again."
            }
        )


def fetch_one(bq: bigquery.Client, query: str, params: Optional[List[Any]] = None):
    rows = list(run_query(bq, query, params))
    return dict(rows[0]) if rows else None


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

    return row


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

    total_income = float(income_row["total_income"])
    total_expenses = float(expense_row["total_expenses"])

    return {
        "property_id": property_id,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_cash_flow": total_income - total_expenses
    }


# -----------------------------------------------------------------------------
# Additional Endpoint 1: Health Check
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "Property Management API is running successfully."
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
    results = run_query(bq, query)
    return [dict(row) for row in results]


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
        bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", payload.monthly_rent),
    ]

    run_query(bq, query, params)

    return {
        "property_id": property_id,
        **payload.model_dump()
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
        bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", payload.monthly_rent),
    ]

    run_query(bq, query, params)

    return {
        "property_id": property_id,
        **payload.model_dump()
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
        ORDER BY income_id
    """

    results = run_query(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [dict(row) for row in results]


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
        bigquery.ScalarQueryParameter("amount", "FLOAT64", payload.amount),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {
        "income_id": income_id,
        "property_id": property_id,
        **payload.model_dump()
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
        ORDER BY expense_id
    """

    results = run_query(
        bq,
        query,
        [bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    return [dict(row) for row in results]


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
        bigquery.ScalarQueryParameter("amount", "FLOAT64", payload.amount),
        bigquery.ScalarQueryParameter("date", "DATE", payload.date),
        bigquery.ScalarQueryParameter("category", "STRING", payload.category),
        bigquery.ScalarQueryParameter("vendor", "STRING", payload.vendor),
        bigquery.ScalarQueryParameter("description", "STRING", payload.description),
    ]

    run_query(bq, query, params)

    return {
        "expense_id": expense_id,
        "property_id": property_id,
        **payload.model_dump()
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