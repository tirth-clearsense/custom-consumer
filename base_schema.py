from sqlalchemy import TIME, BigInteger, Boolean, Column, Float, TIMESTAMP
from sqlalchemy.types import Integer, Numeric, String, ARRAY
base_schema = {
    "integer_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Integer),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "integer_interval_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "start_time": Column(TIMESTAMP, primary_key=True),
        "end_time": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Integer),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "numeric_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Float),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "numeric_interval_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "start_time": Column(TIMESTAMP, primary_key=True),
        "end_time": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Float),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "string_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(String),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "user_datastreams_store.avsc": {
        "individual_id": Column(String, primary_key=True),
        "source": Column(String, primary_key=True),
        "datastream": Column(String, primary_key=True),
        "last_updated": Column(TIMESTAMP)
    },
    "user_info.avsc": {
        "user_id": Column(String, primary_key=True),
        "is_physician": Column(Boolean, nullable=False, default=False),
        "email": Column(String,nullable=False),
        "name": Column(String, nullable=False),
        "first_name": Column(String),
        "last_name": Column(String),
        "address": Column(String,default=None),
        "city": Column(String,default=None),
        "country": Column(String,default=None),
        "postal_code": Column(String,nullable=False),
        "about": Column(String,default=None),
        "provider": Column(String,default=None),
        "created_at": Column(TIMESTAMP),
        "updated_at": Column(TIMESTAMP)
    },
    "json_object_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(JSON),
        "unit": Column(String),
        "confidence": Column(String, default=None)
    }
}
