from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource
from feast.value_type import ValueType
from feast.types import Float64, Int64


card = Entity(name="card", join_keys=["card_id"], value_type=ValueType.STRING)

card_velocity_1m_batch_source = FileSource(
    path="data/card_velocity_1m_v1.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

card_velocity_push_source = PushSource(
    name="card_velocity_1m_push_source",
    batch_source=card_velocity_1m_batch_source,
)


card_velocity_1m_view = FeatureView(
    name="card_velocity_1m_v1",
    entities=[card],
    ttl=timedelta(minutes=120),
    schema=[
        Field(name="txn_count_1m", dtype=Int64),
        Field(name="amount_sum_1m", dtype=Float64),
    ],
    source=card_velocity_push_source,
    online=True,
)

