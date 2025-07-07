import pandas as pd
from pandas.tseries.offsets import DateOffset

def transform_for_product(df_period: pd.DataFrame, product: str) -> pd.Series:
    sum_col = f'sum_{product}'
    count_col = f'count_{product}'
    
    # Группируем по клиенту и считаем общую сумму и количество за период
    agg = df_period.groupby('id').agg({
        sum_col: 'sum',
        count_col: 'sum'
    }).reset_index()
    
    # Расчет флага активности
    flag_col = f'flag_{product}'
    agg[flag_col] = ((agg[sum_col] > 0) & (agg[count_col] > 0)).astype(int)
    
    # Устанавливаем 'id' как индекс для удобного мержа
    return agg.set_index('id')[flag_col]


def transform(df: pd.DataFrame, date: str) -> pd.DataFrame:
    # Преобразование строковых дат в datetime объекты для корректной фильтрации
    df['date'] = pd.to_datetime(df['date'])
    report_date = pd.to_datetime(date)
    
    # Определение периода для анализа: отчетная дата и два предыдущих месяца
    start_period = report_date - DateOffset(months=2)
    
    # Фильтрация данных за последние 3 месяца
    df_period = df[(df['date'] >= start_period) & (df['date'] <= report_date)].copy()
    
    products = [chr(ord('a') + i) for i in range(10)]
    
    # Группировка по клиентам и агрегация данных за период
    aggregated_data = []
    for product in products:
        product_series = transform_for_product(df_period.copy(), product)
        aggregated_data.append(product_series)

    # Объединение флагов по всем продуктам в одну таблицу
    result_df = pd.concat(aggregated_data, axis=1).reset_index()
        
    result_df['date'] = report_date.strftime('%Y-%m-%d')
    
    # Упорядочивание колонок для финального вывода
    column_order = ['id', 'date'] + [f'flag_{p}' for p in products]
    result_df = result_df[column_order]
    
    return result_df
