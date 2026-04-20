import json
import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


def _client() -> bigquery.Client:
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(credentials=credentials, project=sa_info["project_id"])


@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    return _client().query(sql).to_dataframe()
