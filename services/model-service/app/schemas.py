from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    event_id: str | None = Field(default=None, min_length=1)
    card_id: str = Field(..., min_length=1)
    amount: float = Field(..., ge=0)
    event_ts: str | None = None
    ingest_ts: str | None = None


class PredictResponse(BaseModel):
    event_id: str
    card_id: str
    fraud_probability: float
    decision: str
    model_version: str

