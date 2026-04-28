from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    card_id: str = Field(..., min_length=1)
    amount: float = Field(..., ge=0)


class PredictResponse(BaseModel):
    card_id: str
    fraud_probability: float
    decision: str
    model_version: str

