from datetime import datetime, timedelta
import argparse
import sys
import pandas as pd
import os
import yaml
import concurrent.futures
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery


# =====================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================================================================================

def _handle_google_ads_exception(ex, customer_id, context="запроса"):
    """Безопасно извлекает и печатает сообщение об ошибке."""
    error_message = f"Неизвестная ошибка: {ex}"
    error_code = "UNKNOWN"
    try:
        error_code = ex.failure.errors[0].error_code.name
        error_message = ex.failure.errors[0].message
    except Exception:
        try:
            error_code = ex.error.code().name
            error_message = str(ex)
        except Exception:
            pass
    print(f'⚠️ Ошибка {context} для аккаунта {customer_id}: {error_code} | {error_message}')


def stream_to_bigquery(dataframe, project_id, table_id, is_first_batch=False):
    """
    Загружает DataFrame в BigQuery.
    """
    if dataframe.empty:
        return False

    # Конвертируем IDs в строки
    for col in ['campaign_id', 'ad_group_id', 'customer_id']:
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].astype(str)

    write_disposition = "WRITE_TRUNCATE" if is_first_batch else "WRITE_APPEND"

    try:
        bq_client = bigquery.Client(project=project_id)
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        # Опция обновления схемы допустима ТОЛЬКО при WRITE_APPEND
        if write_disposition == "WRITE_APPEND":
            job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        job = bq_client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        job.result()
        print(f"   >>> [BQ] Загружено {len(dataframe)} строк в {table_id} ({write_disposition})")
        return True
    except Exception as e:
        print(f"❌ Критическая ошибка при загрузке в BigQuery ({table_id}): {e}")
        return False


# =====================================================================================
# ЛОГИКА ПОЛУЧЕНИЯ ДАННЫХ
# =====================================================================================

def get_active_client_accounts(client, mcc_id):
    """Получает список активных дочерних аккаунтов."""
    ga_service = client.get_service("GoogleAdsService")
    print(f"🔍 Поиск активных аккаунтов в MCC {mcc_id}...")

    query = """
        SELECT 
            customer_client.client_customer, 
            customer_client.descriptive_name 
        FROM customer_client 
        WHERE 
            customer_client.status = 'ENABLED' 
            AND customer_client.manager = FALSE
    """

    all_accounts = {}
    try:
        response = ga_service.search_stream(customer_id=mcc_id, query=query)
        for batch in response:
            for row in batch.results:
                c_id = row.customer_client.client_customer.split('/')[-1]
                c_name = row.customer_client.descriptive_name
                all_accounts[c_id] = c_name
        print(f"   Всего найдено аккаунтов: {len(all_accounts)}")
    except GoogleAdsException as ex:
        _handle_google_ads_exception(ex, mcc_id, "поиска клиентов")
        sys.exit(1)

    return [{"id": k, "name": v} for k, v in all_accounts.items()]


def load_pmax_data(client, customer_id, account_name, start_date_str, end_date_str):
    ga_service = client.get_service("GoogleAdsService")

    query_general = f"""
        SELECT 
            campaign.id,
            campaign.name, 
            campaign.status,
            campaign_budget.amount_micros,
            segments.date, 
            metrics.clicks, 
            metrics.impressions, 
            metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
    """
    query_conv = f"""
        SELECT campaign.id, segments.date, segments.conversion_action_name, metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
    """

    pmax_df = pd.DataFrame()
    conv_df = pd.DataFrame()

    try:
        resp_gen = ga_service.search_stream(customer_id=customer_id, query=query_general)
        rows_gen = []
        for batch in resp_gen:
            for r in batch.results:
                rows_gen.append({
                    "account_name": account_name,
                    "campaign_id": r.campaign.id,
                    "campaign_name": r.campaign.name,
                    "campaign_status": r.campaign.status.name,
                    "daily_budget": r.campaign_budget.amount_micros / 1000000,
                    "ad_group_id": "0",
                    "ad_group_name": "Performance Max",
                    "date": r.segments.date,
                    "clicks": r.metrics.clicks,
                    "impressions": r.metrics.impressions,
                    "cost": r.metrics.cost_micros / 1000000
                })
        pmax_df = pd.DataFrame(rows_gen)

        resp_conv = ga_service.search_stream(customer_id=customer_id, query=query_conv)
        rows_conv = [
            {"campaign_id": r.campaign.id, "date": r.segments.date,
             "conversion_name": r.segments.conversion_action_name, "conversions": r.metrics.conversions}
            for b in resp_conv for r in b.results
        ]
        conv_df = pd.DataFrame(rows_conv)

    except GoogleAdsException as ex:
        _handle_google_ads_exception(ex, customer_id, "PMax")
        return pd.DataFrame()

    if pmax_df.empty and conv_df.empty:
        return pd.DataFrame()
    elif pmax_df.empty:
        final = conv_df
        final['account_name'] = account_name
        final['ad_group_name'] = "Performance Max"
    elif conv_df.empty:
        final = pmax_df
    else:
        final = pd.merge(pmax_df, conv_df, on=["campaign_id", "date"], how="outer")
        if 'account_name' in final.columns:
            final['account_name'] = final['account_name'].fillna(account_name)

    return final


def get_all_campaign_data_for_account(client, customer_id, account_name, start_date_str, end_date_str):
    ga_service = client.get_service("GoogleAdsService")

    query_budgets = f"""
        SELECT campaign.id, campaign_budget.amount_micros 
        FROM campaign 
        WHERE campaign.status != 'REMOVED'
    """

    campaign_budgets = {}
    try:
        resp_b = ga_service.search_stream(customer_id=customer_id, query=query_budgets)
        for batch in resp_b:
            for r in batch.results:
                campaign_budgets[str(r.campaign.id)] = r.campaign_budget.amount_micros / 1000000
    except GoogleAdsException:
        pass

    query_std = f"""
        SELECT 
            campaign.id,
            campaign.name, 
            campaign.status,
            ad_group.id,
            ad_group.name, 
            segments.date, 
            metrics.clicks, 
            metrics.impressions, 
            metrics.cost_micros
        FROM ad_group 
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}' 
        AND campaign.advertising_channel_type != 'PERFORMANCE_MAX'
    """

    query_std_conv = f"""
        SELECT campaign.id, ad_group.id, segments.date, segments.conversion_action_name, metrics.conversions
        FROM ad_group WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}' AND campaign.advertising_channel_type != 'PERFORMANCE_MAX'
    """

    std_df = pd.DataFrame()
    conv_df = pd.DataFrame()

    try:
        resp_std = ga_service.search_stream(customer_id=customer_id, query=query_std)
        rows_std = []
        for batch in resp_std:
            for r in batch.results:
                camp_id_str = str(r.campaign.id)
                budget_val = campaign_budgets.get(camp_id_str, 0)

                rows_std.append({
                    "account_name": account_name,
                    "campaign_id": camp_id_str,
                    "campaign_name": r.campaign.name,
                    "campaign_status": r.campaign.status.name,
                    "daily_budget": budget_val,
                    "ad_group_id": str(r.ad_group.id),
                    "ad_group_name": r.ad_group.name,
                    "date": r.segments.date,
                    "clicks": r.metrics.clicks,
                    "impressions": r.metrics.impressions,
                    "cost": r.metrics.cost_micros / 1000000
                })
        std_df = pd.DataFrame(rows_std)

        resp_conv = ga_service.search_stream(customer_id=customer_id, query=query_std_conv)
        rows_conv = [
            {"campaign_id": str(r.campaign.id), "ad_group_id": str(r.ad_group.id), "date": r.segments.date,
             "conversion_name": r.segments.conversion_action_name, "conversions": r.metrics.conversions}
            for b in resp_conv for r in b.results
        ]
        conv_df = pd.DataFrame(rows_conv)

    except GoogleAdsException as ex:
        _handle_google_ads_exception(ex, customer_id, "Standard Campaigns")

    if std_df.empty and conv_df.empty:
        final_std = pd.DataFrame()
    elif std_df.empty:
        final_std = conv_df
        final_std['account_name'] = account_name
    elif conv_df.empty:
        final_std = std_df
    else:
        final_std = pd.merge(std_df, conv_df, on=["campaign_id", "ad_group_id", "date"], how="outer")
        final_std['account_name'] = final_std['account_name'].fillna(account_name)

    final_pmax = load_pmax_data(client, customer_id, account_name, start_date_str, end_date_str)
    final_df = pd.concat([final_std, final_pmax], ignore_index=True)

    if not final_df.empty:
        cols_zero = ['clicks', 'impressions', 'cost', 'conversions', 'daily_budget']
        for col in cols_zero:
            if col in final_df.columns:
                final_df[col] = final_df[col].fillna(0)

        final_df['customer_id'] = customer_id
        if 'campaign_status' in final_df.columns:
            final_df['campaign_status'] = final_df['campaign_status'].fillna('UNKNOWN')

    return final_df


def get_geo_data_for_account(client, customer_id, account_name, start_date_str, end_date_str):
    ga_service = client.get_service("GoogleAdsService")
    query_geo = f"""
        SELECT 
            campaign.id,
            campaign.name, 
            segments.date, 
            geographic_view.country_criterion_id, 
            metrics.impressions, 
            metrics.clicks, 
            metrics.cost_micros, 
            metrics.conversions
        FROM geographic_view WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query_geo)
        rows = [{
            "account_name": account_name,
            "customer_id": customer_id,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "date": r.segments.date,
            "country_criterion_id": r.geographic_view.country_criterion_id,
            "impressions": r.metrics.impressions,
            "clicks": r.metrics.clicks,
            "cost": r.metrics.cost_micros / 1000000,
            "conversions": r.metrics.conversions
        } for b in response for r in b.results]
        return pd.DataFrame(rows)
    except GoogleAdsException as ex:
        _handle_google_ads_exception(ex, customer_id, "GEO")
        return pd.DataFrame()


def get_search_query_data_for_account(client, customer_id, account_name, start_date_str, end_date_str):
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT 
            segments.date, 
            campaign.id,
            campaign.name, 
            ad_group.id,
            ad_group.name,
            search_term_view.search_term, 
            segments.device, 
            metrics.impressions, 
            metrics.clicks, 
            metrics.cost_micros, 
            metrics.conversions
        FROM search_term_view
        WHERE segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        rows = []
        for batch in response:
            for row in batch.results:
                rows.append({
                    "account_name": account_name,
                    "customer_id": customer_id,
                    "date": row.segments.date,
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "ad_group_id": row.ad_group.id,
                    "ad_group_name": row.ad_group.name,
                    "search_term": row.search_term_view.search_term,
                    "device": row.segments.device.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "cost": row.metrics.cost_micros / 1000000,
                    "conversions": row.metrics.conversions,
                })
        return pd.DataFrame(rows)
    except GoogleAdsException as ex:
        _handle_google_ads_exception(ex, customer_id, "Search Query")
        return pd.DataFrame()


# =====================================================================================
# WRAPPER ДЛЯ ПОТОКОВ
# =====================================================================================
def process_one_account(client, account, start_str, end_str):
    """
    Собирает все данные для одного аккаунта.
    Возвращает словарь с DataFrame'ами.
    """
    c_id = account['id']
    c_name = account['name']

    print(f"   ⏳ Start: {c_name}")

    df_camp = get_all_campaign_data_for_account(client, c_id, c_name, start_str, end_str)
    df_geo = get_geo_data_for_account(client, c_id, c_name, start_str, end_str)
    df_search = get_search_query_data_for_account(client, c_id, c_name, start_str, end_str)

    print(f"   ✅ Done: {c_name}")

    return {
        "name": c_name,
        "camp": df_camp,
        "geo": df_geo,
        "search": df_search
    }


# =====================================================================================
# MAIN
# =====================================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MCC скрипт выгрузки (Parallel).")
    parser.add_argument("--mcc_id", type=str, required=True, help="ID MCC аккаунта.")
    parser.add_argument("-p", "--project_id", type=str, required=True, help="ID проекта Google Cloud.")
    parser.add_argument("-d", "--dataset_id", type=str, required=True, help="ID датасета в BigQuery.")
    parser.add_argument("--config_file", type=str, required=True, help="Путь к googleads.yaml.")
    parser.add_argument("--key_file", type=str, required=True, help="Путь к service_key.json.")
    args = parser.parse_args()

    if not os.path.exists(args.key_file):
        print(f"Ошибка: Файл ключа {args.key_file} не найден.")
        sys.exit(1)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key_file

    try:
        with open(args.config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        config_data['login_customer_id'] = args.mcc_id

        # --- ВАЖНОЕ ИСПРАВЛЕНИЕ ---
        # Перезаписываем путь к JSON-ключу в конфиге
        # Иначе Google Ads пытается взять C:\Users... из YAML файла
        config_data['json_key_file_path'] = args.key_file

        googleads_client = GoogleAdsClient.load_from_dict(config_data)
        print(f"✅ Клиент инициализирован. MCC Context: {args.mcc_id}")
    except Exception as e:
        print(f"❌ Ошибка инициализации клиента: {e}")
        sys.exit(1)

    # ДАТЫ: 90 ДНЕЙ
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=90)
    s_str = start_date.strftime('%Y-%m-%d')
    e_str = end_date.strftime('%Y-%m-%d')
    print(f"📅 Период: {s_str} - {e_str} (90 дней)")

    # Вызываем функцию БЕЗ второго аргумента
    accounts_list = get_active_client_accounts(googleads_client, args.mcc_id)

    tbl_main = f"{args.project_id}.{args.dataset_id}.daily_campaign_data"
    tbl_geo = f"{args.project_id}.{args.dataset_id}.daily_geo_data"
    tbl_search = f"{args.project_id}.{args.dataset_id}.daily_search_query_data"

    first_run_main = True
    first_run_geo = True
    first_run_search = True

    # --- ЗАПУСК В ПОТОКАХ (MAX 10) ---
    # Мы собираем данные параллельно, а загружаем последовательно,
    # чтобы не ловить ошибки BQ и корректно управлять WRITE_TRUNCATE

    print(f"\n🚀 Запуск обработки {len(accounts_list)} аккаунтов в 10 потоков...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Создаем словарь {future: account_info} для отслеживания
        futures = {
            executor.submit(process_one_account, googleads_client, acc, s_str, e_str): acc
            for acc in accounts_list
        }

        # Обрабатываем по мере завершения (as_completed)
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()

                # 1. Загрузка Main
                if not result['camp'].empty:
                    success = stream_to_bigquery(result['camp'], args.project_id, tbl_main,
                                                 is_first_batch=first_run_main)
                    if success and first_run_main:
                        first_run_main = False  # Флаг снимается только после первой удачной загрузки

                # 2. Загрузка Geo
                if not result['geo'].empty:
                    success = stream_to_bigquery(result['geo'], args.project_id, tbl_geo, is_first_batch=first_run_geo)
                    if success and first_run_geo:
                        first_run_geo = False

                # 3. Загрузка Search
                if not result['search'].empty:
                    success = stream_to_bigquery(result['search'], args.project_id, tbl_search,
                                                 is_first_batch=first_run_search)
                    if success and first_run_search:
                        first_run_search = False

            except Exception as exc:
                print(f"❌ Сбой в потоке: {exc}")

    print("\n🎉 Полная выгрузка завершена.")
