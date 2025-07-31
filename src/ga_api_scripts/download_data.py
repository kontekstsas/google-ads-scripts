from datetime import datetime, timedelta
import argparse
import sys
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField  # Для определения схемы BigQuery




# --- ФУНКЦИЯ 1: ПОЛУЧЕНИЕ ОСНОВНЫХ ДАННЫХ ИЗ GOOGLE ADS API ---
def get_basic_ad_performance_data(client, customer_id, start_date_str, end_date_str):
   """
   Загружает базовые данные по кампаниям и группам объявлений (показы, клики, стоимость)
   из Google Ads API, сегментированные по дате.
   Возвращает pandas DataFrame.
   """
   ga_service = client.get_service("GoogleAdsService")
   print(
       f"\n1. Запрос базовых данных (показы, клики, стоимость) из Google Ads API за период с {start_date_str} по {end_date_str}...")


   # ЕДИНЫЙ GAQL запрос для всех нужных метрик
   query = f"""
       SELECT
           campaign.id,
           campaign.name,
           ad_group.id,
           ad_group.name,
           segments.date,
           metrics.impressions,
           metrics.clicks,
           metrics.cost_micros # Стоимость в микро-единицах
       FROM
           ad_group
       WHERE
           segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
       ORDER BY
           segments.date ASC, campaign.id ASC, ad_group.id ASC
   """


   results_list = []
   try:
       stream = ga_service.search_stream(customer_id=customer_id, query=query)


       for batch in stream:
           for row in batch.results:
               results_list.append({
                   "date": row.segments.date,
                   "campaign_id": row.campaign.id,
                   "campaign_name": row.campaign.name,
                   "ad_group_id": row.ad_group.id,
                   "ad_group_name": row.ad_group.name,
                   "impressions": row.metrics.impressions,
                   "clicks": row.metrics.clicks,
                   "cost": row.metrics.cost_micros / 1_000_000  # Переводим из микро-единиц
               })


       df = pd.DataFrame(results_list)
       print(f"Получено {len(df)} строк данных из Google Ads.")
       return df


   except GoogleAdsException as ex:
       print(f'Ошибка запроса Google Ads API (ID: "{ex.request_id}"): {ex.error.code().name}')
       print(f"Сообщение: {ex.error.message}")
       sys.exit(1)
   except Exception as e:
       print(f"Непредвиденная ошибка при получении данных: {e}")
       sys.exit(1)


   return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки




# --- ФУНКЦИЯ 2: ЗАГРУЗКА ДАННЫХ В BIGQUERY (С АВТОСОЗДАНИЕМ ТАБЛИЦЫ) ---
def load_data_to_bigquery(df, project_id, table_id):
   """
   Загружает pandas DataFrame в указанную таблицу BigQuery.
   Автоматически создает таблицу, если она не существует.
   """
   if df.empty:
       print("Нет данных для загрузки в BigQuery.")
       return


   bq_client = bigquery.Client(project=project_id)


   # Разбиваем table_id на dataset и table name
   try:
       dataset_id = table_id.split('.')[1]
       table_name = table_id.split('.')[2]
       dataset_ref = bq_client.dataset(dataset_id)
       table_ref = dataset_ref.table(table_name)
   except IndexError:
       print(f"Ошибка: Неверный формат 'table_id'. Ожидается 'project.dataset.table'. Получено: '{table_id}'")
       sys.exit(1)


   print(f"\n2. Проверка и загрузка данных в таблицу BigQuery: {table_id}...")


   try:
       # Проверяем, существует ли таблица
       bq_client.get_table(table_ref)
       print(f"Таблица '{table_id}' уже существует.")
       job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Перезаписываем существующую
   except Exception as e:  # Catch all exceptions if table does not exist
       # Если таблица не существует, создаем ее
       print(f"Таблица '{table_id}' не найдена. Создаем новую.")


       # Автоматическое определение схемы из DataFrame
       schema = []
       for col_name, dtype in df.dtypes.items():
           if col_name == "date":  # Специальная обработка для столбца 'date'
               schema.append(SchemaField(col_name, "DATE", mode="NULLABLE"))
           elif "id" in col_name.lower():  # Для ID (Campaign ID, Ad Group ID)
               schema.append(SchemaField(col_name, "INTEGER", mode="NULLABLE"))
           elif "int" in str(dtype):
               schema.append(SchemaField(col_name, "INTEGER", mode="NULLABLE"))
           elif "float" in str(dtype):
               schema.append(SchemaField(col_name, "FLOAT", mode="NULLABLE"))
           elif "object" in str(dtype):  # pandas 'object' обычно для строк
               schema.append(SchemaField(col_name, "STRING", mode="NULLABLE"))
           else:
               schema.append(SchemaField(col_name, "STRING", mode="NULLABLE"))  # По умолчанию для неизвестных типов


       table = bigquery.Table(table_ref, schema=schema)
       bq_client.create_table(table)
       print(
           f"Таблица '{table_id}' успешно создана со схемой: {[field.name + ':' + field.field_type for field in schema]}")
       job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # После создания также перезаписываем


   # Загружаем данные
   try:
       # Убедимся, что столбец 'date' имеет правильный тип для BigQuery DATE
       if 'date' in df.columns:
           df['date'] = pd.to_datetime(df['date']).dt.date


       job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
       job.result()  # Дождитесь завершения задания
       print(f"🎉 Данные успешно загружены в BigQuery!")


   except Exception as e:
       print(f"Ошибка при загрузке данных в BigQuery: {e}")
       sys.exit(1)




# =====================================================================
# ОСНОВНОЙ БЛОК ЗАПУСКА ПРОГРАММЫ
# =====================================================================
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Загружает базовые данные Google Ads в Google BigQuery.")
   parser.add_argument("-c", "--customer_id", type=str, required=True, help="ID клиента Google Ads (без дефисов).")
   parser.add_argument("-p", "--project_id", type=str, required=True, help="ID проекта Google Cloud.")
   parser.add_argument("-t", "--table_id", type=str, required=True,
                       help="Полный ID таблицы в BigQuery (например, 'my_project.my_dataset.my_table').")
   parser.add_argument("--config_file", type=str, required=True, help="Полный путь к файлу google-ads.yaml.")
   args = parser.parse_args()


   print("--- Запуск выгрузки базовых данных из Google Ads API в BigQuery ---")


   try:
       googleads_client = GoogleAdsClient.load_from_storage(path=args.config_file, version="v18")
       print("Клиент Google Ads успешно инициализирован.")
   except Exception as e:
       print(f"Не удалось инициализировать клиент Google Ads. Проверьте 'google-ads.yaml' и путь к нему.")
       print(f"Ошибка: {e}")
       sys.exit(1)


   # Вычисляем даты
   today = datetime.now()
   end_date = today - timedelta(days=1)
   start_date = today - timedelta(days=90)
   start_date_str = start_date.strftime('%Y-%m-%d')
   end_date_str = end_date.strftime('%Y-%m-%d')
   print(f"Выгружаем данные за период с {start_date_str} по {end_date_str}")


   # Получаем базовые данные
   performance_df = get_basic_ad_performance_data(
       googleads_client, args.customer_id, start_date_str, end_date_str
   )


   # Загружаем данные в BigQuery
   if not performance_df.empty:
       load_data_to_bigquery(performance_df, args.project_id, args.table_id)
   else:
       print("Базовые данные не получены из Google Ads, загрузка в BigQuery пропущена.")


   print("\n--- Программа завершена ---")
