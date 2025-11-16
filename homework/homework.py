"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


import pandas as pd
import os


def _load_campaign_data(input_folder: str = "files/input/") -> pd.DataFrame:
    """Load and concatenate all compressed CSV files."""
    all_files = [
        os.path.join(input_folder, f)
        for f in os.listdir(input_folder)
        if f.endswith(".csv.zip")
    ]
    df_list = [pd.read_csv(f, compression="zip") for f in all_files]
    return pd.concat(df_list, ignore_index=True)


def _clean_client_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean and process client data."""
    client_df = data[
        ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    ].copy()
    
    client_df["job"] = (
        client_df["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )
    client_df["education"] = (
        client_df["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )
    client_df["credit_default"] = (client_df["credit_default"] == "yes").astype(int)
    client_df["mortgage"] = (client_df["mortgage"] == "yes").astype(int)
    
    return client_df


def _clean_campaign_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean and process campaign data."""
    campaign_df = data[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()
    
    campaign_df["previous_outcome"] = (campaign_df["previous_outcome"] == "success").astype(int)
    campaign_df["campaign_outcome"] = (campaign_df["campaign_outcome"] == "yes").astype(int)
    
    campaign_df["last_contact_date"] = pd.to_datetime(
        "2022-" + campaign_df["month"] + "-" + campaign_df["day"].astype(str),
        format="%Y-%b-%d",
    ).dt.strftime("%Y-%m-%d")
    
    campaign_df = campaign_df.drop(columns=["day", "month"])
    
    return campaign_df


def _clean_economics_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean and process economics data."""
    return data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()


def _save_dataframes(client_df: pd.DataFrame, campaign_df: pd.DataFrame, 
                     economics_df: pd.DataFrame, output_folder: str = "files/output/") -> None:
    """Save cleaned dataframes to CSV files."""
    os.makedirs(output_folder, exist_ok=True)
    client_df.to_csv(os.path.join(output_folder, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_folder, "economics.csv"), index=False)


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.
    """
    data = _load_campaign_data()
    
    client_df = _clean_client_data(data)
    campaign_df = _clean_campaign_data(data)
    economics_df = _clean_economics_data(data)
    
    _save_dataframes(client_df, campaign_df, economics_df)


if __name__ == "__main__":
    clean_campaign_data()