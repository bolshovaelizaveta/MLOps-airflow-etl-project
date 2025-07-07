import pandas as pd
from pandas.tseries.offsets import DateOffset

def transform(df: pd.DataFrame, date: str) -> pd.DataFrame:
    # Рассчитывает флаги активности клиентов по продуктам на основе данных за 3 месяца
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
        sum_col = f'sum_{product}'
        count_col = f'count_{product}'
        
        agg = df_period.groupby('id').agg({
            sum_col: 'sum',
            count_col: 'sum'
        }).reset_index()
        
        # Расчет флага активности: ненулевая сумма и количество транзакций за период
        flag_col = f'flag_{product}'
        agg[flag_col] = ((agg[sum_col] > 0) & (agg[count_col] > 0)).astype(int)
        
        aggregated_data.append(agg[['id', flag_col]])
    
    # Объединение флагов по всем продуктам в одну таблицу
    result_df = aggregated_data[0]
    for i in range(1, len(aggregated_data)):
        result_df = pd.merge(result_df, aggregated_data[i], on='id', how='outer')
        
    result_df['date'] = report_date.strftime('%Y-%m-%d')
    
    # Упорядочивание колонок для финального вывода
    column_order = ['id', 'date'] + [f'flag_{p}' for p in products]
    result_df = result_df[column_order]
    
    return result_df
