-- функция возвращает средний чек с учетом коэффициента увеличения (rate_measure_check) - расчет среднего чека
-- производится по транзакциям в отрезке времени между первой и второй датами
CREATE OR REPLACE FUNCTION get_measure_check_between_dates(first_date timestamp, last_date timestamp, rate_measure_check numeric)
RETURNS table(customer_id bigint, goal_check numeric)
LANGUAGE plpgsql
AS $$ BEGIN
RETURN QUERY (
    SELECT
        p.customer_id,
        avg(p.group_summ_paid) * rate_measure_check as goal_check
    FROM purchase_history p
    WHERE p.transaction_datetime >= first_date AND p.transaction_datetime <= last_date
    GROUP BY p.customer_id, rate_measure_check
    );
END; $$;


-- функция возвращает средний чек с учетом коэффициента увеличения (rate_measure_check) - расчет среднего чека
-- производится по количеству транзакций (count)
CREATE OR REPLACE FUNCTION get_measure_check_by_count_transactions(count int, rate_measure_check numeric)
RETURNS table(customer_id bigint, goal_check numeric)
LANGUAGE plpgsql
AS $$ BEGIN
RETURN QUERY (
    WITH statistics as (
        SELECT
            p.customer_id,
            p.group_summ_paid,
            row_number() OVER (PARTITION BY p.customer_id ORDER BY p.transaction_datetime DESC) as rank_transactions
        FROM purchase_history p
    )
    SELECT
        s.customer_id,
        avg(s.group_summ_paid) * rate_measure_check as goal_check
    FROM statistics s
    WHERE s.rank_transactions <= count
    GROUP BY s.customer_id
    );
END; $$;


-- функция возвращает дату самой первой транзакции в датасете "Транзакции"
CREATE OR REPLACE FUNCTION get_min_date_of_transaction()
RETURNS timestamp
LANGUAGE plpgsql
AS $$ BEGIN
    RETURN (SELECT min(transaction_datetime) FROM transactions);
END; $$;




-- функция возвращает таблицу для формирование персональных предложений,
-- ориентированных на рост среднего чека
CREATE OR REPLACE FUNCTION get_offers_average_check(
    method int DEFAULT 1, -- метод расчета среднего чека (1 - за период, 2 - за количество)
    first_date timestamp DEFAULT get_min_date_of_transaction(), -- первая и последняя даты периода (для 1 метода)
    last_date timestamp DEFAULT get_date_analysis(),
    transactions_count int DEFAULT 100, -- количество транзакций (для 2 метода)
    rate_measure_check numeric DEFAULT 1.1, -- коэффициент увеличения среднего чека
    max_churn_rate numeric DEFAULT 300, -- максимальный индекс оттока
    max_discount_share numeric DEFAULT 50, -- максимальная доля транзакций со скидкой (в процентах)
    margin_part numeric DEFAULT 80) -- допустимая доля маржи (в процентах)
RETURNS table(customer_id BIGINT,
        required_check_measure numeric, -- Целевое значение среднего чека
        group_name varchar, -- Группа предложения
        offer_discount_depth int) -- Максимальная глубина скидки
LANGUAGE plpgsql
AS $$
DECLARE
    date_analysis timestamp := get_date_analysis();
BEGIN
    DROP TABLE IF EXISTS goal_measure_checks;
    CREATE TEMP TABLE goal_measure_checks(
        t_customer_id BIGINT,
        t_goal_check numeric);

    IF method = 1 THEN
        IF last_date > date_analysis OR last_date ISNULL THEN
            last_date := date_analysis;
        END IF;

        IF first_date > last_date OR first_date ISNULL THEN
            first_date := get_min_date_of_transaction();
        END IF;
        INSERT INTO goal_measure_checks SELECT * FROM
                    get_measure_check_between_dates(first_date, last_date, rate_measure_check);
    ELSE
        INSERT INTO goal_measure_checks SELECT * FROM
                    get_measure_check_by_count_transactions(transactions_count, rate_measure_check);
    END IF;

RETURN QUERY
    WITH get_customers_checks_groups_discounts as (
    SELECT DISTINCT
         g.customer_id,
         (SELECT gm.t_goal_check FROM goal_measure_checks gm WHERE gm.t_customer_id = g.customer_id) as required_check_measure,
         first_value(g.group_id) OVER w as group_id,
         ((first_value(g.group_minimum_discount) OVER w * 100)::int / 5 + 1) * 5 as offer_discount_depth
    FROM groups g
    WHERE g.group_churn_rate <= max_churn_rate
        AND g.group_discount_share * 100 < max_discount_share
        AND (g.group_minimum_discount * 100)::int / 5 * 5 + 5
            < (SELECT sum(s2.sku_retail_price - s2.sku_purchase_price) / sum(s2.sku_retail_price)
                FROM sku s
                JOIN stores s2 ON g.group_id = s.group_id
                    AND s.sku_id = s2.sku_id) * margin_part
    WINDOW w as (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC))

    SELECT
        g.customer_id,
        g.required_check_measure,
        gs.group_name,
        g.offer_discount_depth
    FROM get_customers_checks_groups_discounts g
    JOIN groups_sku gs ON g.group_id = gs.group_id
    ORDER BY g.customer_id;
END; $$;




------------- ТЕСТОВЫЕ ЗАПРОСЫ ------------------

SELECT * FROM get_offers_average_check();

SELECT * FROM get_offers_average_check(1, NULL, NULL,
    100, 1.15, 3,70, 30);

SELECT * FROM get_offers_average_check(1, '2021-01-01', '2022-06-30',
    100, 1.15, 3,70, 30);

SELECT * FROM get_offers_average_check(2, '2021-01-01', '2022-06-30',
    20, 1.2, 5,50, 50);