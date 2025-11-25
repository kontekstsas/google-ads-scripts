from datetime import datetime, timedelta
import argparse
import sys
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery


# =====================================================================================
# НОВАЯ ФУНКЦИЯ: ЗАГРУЗКА ДАННЫХ ТОЛЬКО ПО PERFORMANCE MAX КАМПАНИЯМ
# =====================================================================================
def load_pmax_data(client, customer_id, start_date_str, end_date_str):
    """Загружает данные по кампаниям Performance Max."""
    ga_service = client.get_service("GoogleAdsService")
    print("\n1.1. Запрос данных по кампаниям Performance Max из Google Ads API...")

    # PMax не имеет групп объявлений, поэтому запрашиваем на уровне кампании
    query_general_pmax = f"""
        SELECT campaign.name, segments.date, metrics.clicks, metrics.cost_micros, metrics.impressions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
    """
    query_conversions_pmax = f"""
        SELECT campaign.name, segments.date, segments.conversion_action_name, metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
    """

    try:
        # Запрос по кликам и стоимости для PMax
        response_general = ga_service.search_stream(customer_id=customer_id, query=query_general_pmax)
        rows_general = [
            {
                "campaign_name": row.campaign.name,
                "ad_group_name": "Performance Max",  # Заглушка, т.к. нет групп
                "date": row.segments.date,
                "clicks": row.metrics.clicks,
                "cost": row.metrics.cost_micros / 1000000,
                "impressions": row.metrics.impressions
            }
            for batch in response_general for row in batch.results
        ]
        pmax_df = pd.DataFrame(rows_general)
        if not pmax_df.empty:
            print(f"Получено {len(pmax_df)} строк с данными по кликам и стоимости для PMax.")

        # Запрос по конверсиям для PMax
        response_conversions = ga_service.search_stream(customer_id=customer_id, query=query_conversions_pmax)
        rows_conversions = [
            {
                "campaign_name": row.campaign.name,
                "ad_group_name": "Performance Max",
                "date": row.segments.date,
                "conversion_name": row.segments.conversion_action_name,
                "conversions": row.metrics.conversions
            }
            for batch in response_conversions for row in batch.results
        ]
        pmax_conversions_df = pd.DataFrame(rows_conversions)
        if not pmax_conversions_df.empty:
            print(f"Получено {len(pmax_conversions_df)} строк с данными по конверсиям для PMax.")

    except GoogleAdsException as ex:
        print(f'Ошибка запроса PMax с ID "{ex.request_id}": {ex.error.code().name}')
        return pd.DataFrame() 

    # Объединение данных PMax
    if pmax_df.empty and pmax_conversions_df.empty:
        print("Данные по Performance Max не найдены.")
        return pd.DataFrame()

    if pmax_df.empty:
        final_pmax_df = pmax_conversions_df
    elif pmax_conversions_df.empty:
        final_pmax_df = pmax_df
    else:
        final_pmax_df = pd.merge(
            pmax_df,
            pmax_conversions_df,
            on=["campaign_name", "ad_group_name", "date"],
            how="outer"
        )

    # Заполняем пропуски нулями
    final_pmax_df[['clicks', 'cost', 'conversions', 'impressions']] = final_pmax_df[['clicks', 'cost', 'conversions', 'impressions']].fillna(0)

    return final_pmax_df


# =====================================================================================
# ФУНКЦИЯ 1: ЗАГРУЗКА ДАННЫХ ПО ВСЕМ КАМПАНИЯМ (ОБЫЧНЫЕ + PMAX)
# =====================================================================================
def load_all_campaign_data(client, customer_id, project_id, table_id, start_date_str, end_date_str):
    """Загружает данные по всем типам кампаний и группам объявлений."""
    ga_service = client.get_service("GoogleAdsService")
    print("\n1. Запрос данных по стандартным кампаниям и группам из Google Ads API...")

    query_general = f"""
        SELECT campaign.name, ad_group.name, segments.date, metrics.clicks, metrics.cost_micros, metrics.impressions
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type != 'PERFORMANCE_MAX'
    """
    query_conversions = f"""
        SELECT campaign.name, ad_group.name, segments.date, segments.conversion_action_name, metrics.conversions
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type != 'PERFORMANCE_MAX'
    """

    try:
        # Выполняем запрос для стандартных кампаний
        response_general = ga_service.search_stream(customer_id=customer_id, query=query_general)
        rows_general = [
            {"campaign_name": row.campaign.name, "ad_group_name": row.ad_group.name, "date": row.segments.date,
             "clicks": row.metrics.clicks, "impressions": row.metrics.impressions, "cost": row.metrics.cost_micros / 1000000} for batch in response_general for
            row in batch.results]
        campaign_df = pd.DataFrame(rows_general)
        print(f"Получено {len(campaign_df)} строк с данными по кликам и стоимости для стандартных кампаний.")

        # Выполняем второй запрос для стандартных кампаний
        response_conversions = ga_service.search_stream(customer_id=customer_id, query=query_conversions)
        rows_conversions = [
            {"campaign_name": row.campaign.name, "ad_group_name": row.ad_group.name, "date": row.segments.date,
             "conversion_name": row.segments.conversion_action_name, "conversions": row.metrics.conversions} for batch
            in response_conversions for row in batch.results]
        conversions_df = pd.DataFrame(rows_conversions)
        print(f"Получено {len(conversions_df)} строк с данными по конверсиям для стандартных кампаний.")

    except GoogleAdsException as ex:
        print(f'Ошибка запроса с ID "{ex.request_id}": {ex.error.code().name}')
        sys.exit(1)

    # Объединение данных по стандартным кампаниям
    print("2. Объединение данных по стандартным кампаниям...")
    if campaign_df.empty and conversions_df.empty:
        standard_final_df = pd.DataFrame()
    elif campaign_df.empty:
        standard_final_df = conversions_df
    elif conversions_df.empty:
        standard_final_df = campaign_df
    else:
        standard_final_df = pd.merge(campaign_df, conversions_df, on=["campaign_name", "ad_group_name", "date"],
                                     how="outer")

    # Получаем данные по PMax, вызвав новую функцию
    pmax_final_df = load_pmax_data(client, customer_id, start_date_str, end_date_str)

    # Объединяем стандартные и PMax данные
    print("3. Объединение данных стандартных и Performance Max кампаний...")
    final_df = pd.concat([standard_final_df, pmax_final_df], ignore_index=True)

    final_df[['clicks', 'cost', 'conversions','impressions']] = final_df[['clicks', 'cost', 'conversions', 'impressions']].fillna(0)

    if final_df.empty:
        print("Нет данных для основной таблицы.")
        return

    # Загрузка в BigQuery
    print(f"4. Загрузка {len(final_df)} строк в таблицу {table_id}...")
    try:
        bq_client = bigquery.Client(project=project_id)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
        job.result()
        print(f"🎉 Данные по всем кампаниям успешно загружены в BigQuery!")
    except Exception as e:
        print(f"Ошибка при загрузке основной таблицы в BigQuery: {e}")
        sys.exit(1)


# =====================================================================================
# ОСНОВНОЙ БЛОК ЗАПУСКА
# =====================================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Загружает данные из Google Ads в Google BigQuery.")
    parser.add_argument("-c", "--customer_id", type=str, required=True, help="ID клиента Google Ads (без дефисов).")
    parser.add_argument("-p", "--project_id", type=str, required=True, help="ID проекта Google Cloud.")
    parser.add_argument("-t", "--table_id", type=str, required=True,
                        help="Полный ID таблицы в BigQuery (например, 'my_project.my_dataset.my_table').")
    parser.add_argument("--config_file", type=str, required=True, help="Полный путь к файлу googleads.yaml.")
    args = parser.parse_args()

    try:
        googleads_client = GoogleAdsClient.load_from_storage(args.config_file,
                                                             version="v21")  # Актуальная версия API
        print("Клиент Google Ads успешно инициализирован.")
    except Exception as e:
        print(f"Не удалось загрузить конфигурационный файл. Ошибка: {e}")
        sys.exit(1)

    # Вычисляем даты один раз
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=90)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    print(f"Выгружаем данные за период с {start_date_str} по {end_date_str}")

    # Вызываем обновленную главную функцию
    load_all_campaign_data(googleads_client, args.customer_id, args.project_id, args.table_id, start_date_str,
                           end_date_str)
