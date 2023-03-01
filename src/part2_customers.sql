-- функция возвращает дату анализа (последнюю строчку из таблицы)
CREATE OR REPLACE FUNCTION get_date_analysis()
RETURNS timestamp
LANGUAGE plpgsql
AS $$ BEGIN
    RETURN (SELECT max(analysis_formation) FROM data_of_analysis_formation);
END; $$;


-- функция возвращает разницу в днях между двумя датами с учетом округления дат до дней
CREATE OR REPLACE FUNCTION get_diff_between_date_in_days(first_date timestamp, last_date timestamp)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    time_interval interval := first_date - last_date;
BEGIN
RETURN abs(date_part('day', time_interval) + date_part('hour', time_interval) / 24 + date_part('minute', time_interval) / (24 * 60)
    + date_part('second', time_interval) / ( 24 * 60 * 60));
END; $$;


-- функция возвращает основной магазин покупателя
CREATE OR REPLACE FUNCTION get_primary_store_id(target_customer_id BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$ BEGIN
RETURN
    (WITH
    -- формируем сводную таблицу по покупателям и магазинам и рассчитываем количество посещений в каждый магазин,
    -- дату последнего посещения этого магазина и ранг, отражающий посещения в обратном хронологическом порядке
    stat_stores AS (
    SELECT
        t.transaction_store_id,
        COUNT(*) OVER w1 AS visits_count,
        MAX(t.transaction_datetime) OVER w1 AS last_visit_date,
        ROW_NUMBER() OVER w2 AS store_rank
    FROM personal_data p
        JOIN cards c ON p.customer_id = c.customer_id
        JOIN transactions t ON c.customer_card_id = t.customer_card_id
    WHERE t.transaction_datetime <= get_date_analysis()
        AND p.customer_id = target_customer_id
    WINDOW w1 AS (PARTITION BY t.transaction_store_id),
           w2 AS (ORDER BY t.transaction_datetime DESC)),

    -- находим самый популярный магазин для каждого покупателя
    get_popular_store AS (
    SELECT DISTINCT
        FIRST_VALUE(transaction_store_id) OVER (ORDER BY visits_count DESC, last_visit_date DESC) AS popular_store_id
     FROM stat_stores),

    -- находим последние 3 посещенных магазина и определяем был ли покупатель именно там за последние три посещения
    get_last_store AS (
    SELECT DISTINCT
        MAX(transaction_store_id) AS last_store_id,
        MAX(transaction_store_id) = MIN(transaction_store_id) AS is_last_store
    FROM stat_stores
    WHERE store_rank <= 3)

    -- получаем основной магазин покупателя
    SELECT
        CASE
            WHEN (SELECT is_last_store FROM get_last_store last) THEN (SELECT last_store_id FROM get_last_store last)
            ELSE (SELECT popular_store_id FROM get_popular_store)
        END AS customer_primary_store_id
    );
END; $$;



-- представление содержит идентификатор покупателя, размер среднего чека, сегмент по среднему чеку, частота транзакций,
-- сегмент по частоте транзакций, количество дней после последней транзакции, коэффициент оттока, сегмент по коэффициенту оттока,
-- итоговый сегмент покупателя, идентификатор основного магазина
CREATE OR REPLACE VIEW customers
AS (

-- вспомогательная таблица для расчета показателей: содержит идентификатор клиента, размер среднего чека,
-- размер среднего интервала между транзакциями, количество дней с предыдущей транзакции
WITH stat_transact AS (
SELECT
    c.customer_id,
    AVG(t.transaction_summ) AS customer_average_check,
    get_diff_between_date_in_days(MAX(t.transaction_datetime), MIN(t.transaction_datetime)) / COUNT(*) AS customer_frequency,
    get_diff_between_date_in_days(get_date_analysis(), MAX(t.transaction_datetime)) AS customer_inactive_period
FROM personal_data p
     JOIN cards c ON p.customer_id = c.customer_id
     JOIN transactions t ON c.customer_card_id = t.customer_card_id
GROUP BY c.customer_id),

-- рассчитываем ранг по среднему чеку, частоте заказов и рассчитываем коэффициент оттока
stat_rank AS (
SELECT
     customer_id,
     customer_average_check,
     CUME_DIST() OVER (ORDER BY customer_average_check) AS rank_check, customer_frequency,
     CUME_DIST() OVER (ORDER BY customer_frequency) AS rank_freq, customer_inactive_period,
     customer_inactive_period / customer_frequency AS customer_churn_rate
FROM stat_transact),

-- определяем сегмент по среднему чеку, частоте заказов и коэффициенту оттока
stat_segment AS (
SELECT
    customer_id,
    customer_average_check,
    CASE
        WHEN rank_check <= 0.1 THEN 'High'
        WHEN rank_check <= 0.35 THEN 'Medium'
        ELSE 'Low' END AS customer_average_check_segment,
    customer_frequency,
    CASE
        WHEN rank_freq <= 0.1 THEN 'Often'
        WHEN rank_freq <= 0.35 THEN 'Occasionally'
        ELSE 'Rarely' END AS customer_frequency_segment,
    customer_inactive_period,
    customer_churn_rate,
    CASE
        WHEN customer_churn_rate < 2 THEN 'Low'
        WHEN customer_churn_rate < 5 THEN 'Medium'
        ELSE 'High' END AS customer_churn_segment
FROM stat_rank)

-- выходная таблица
SELECT a.customer_id,
       a.customer_average_check,
       a.customer_average_check_segment,
       a.customer_frequency,
       a.customer_frequency_segment,
       a.customer_inactive_period,
       a.customer_churn_rate,
       a.customer_churn_segment,
       CASE customer_average_check_segment WHEN 'Low' THEN 0 WHEN 'Medium' THEN 9 ELSE 18 END
        + CASE customer_frequency_segment WHEN 'Rarely' THEN 0 WHEN 'Occasionally' THEN 3 ELSE 6 END
        + CASE customer_churn_segment WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END AS customer_segment,
       get_primary_store_id(a.customer_id) as customer_primary_store
FROM stat_segment a
ORDER BY a.customer_id );




------------------  ТЕСТОВЫЕ ЗАПРОСЫ  ---------------------

SELECT * FROM customers;

SELECT * FROM customers WHERE customer_id IN (1, 3, 5, 368);

SELECT * FROM customers WHERE customer_segment > 3;

SELECT * FROM customers WHERE customer_churn_segment = 'Low';

SELECT * FROM customers WHERE customer_churn_segment = 'Medium';


