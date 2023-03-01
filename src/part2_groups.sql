-- функция возвращает индекса востребованности группы: количество транзакций с анализируемой группой
-- (значение поля Group_Purchase таблицы Периоды) делится на общее количество транзакций клиента,
-- совершенных с первой по последнюю транзакции, в которых была анализируемая группа
CREATE OR REPLACE FUNCTION get_group_affinity_index(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$ BEGIN
RETURN (
    SELECT
        (SELECT p.group_purchase::numeric / count(DISTINCT ph.transaction_id) FROM purchase_history ph
         WHERE ph.customer_id = t_customer_id
         AND ph.transaction_datetime BETWEEN p.first_group_purchase_date AND p.last_group_purchase_date)
    FROM periods p
    WHERE p.customer_id = t_customer_id AND p.group_id = t_group_id
    );
END; $$;
-- SELECT get_group_affinity_index(1, 1);


-- функция возвращает индекс оттока: количество дней, прошедших после даты последней транзакции
-- клиента с анализируемой группой, делится на среднее количество дней между покупками анализируемой группы клиентом
CREATE OR REPLACE FUNCTION get_group_churn_rate(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$ BEGIN
RETURN (
    SELECT
        get_diff_between_date_in_days(get_date_analysis(), p.last_group_purchase_date) / p.group_frequency
    FROM periods p
    WHERE p.customer_id = t_customer_id AND p.group_id = t_group_id
    );
END; $$;
-- SELECT get_group_churn_rate(1, 1);


-- функция возвращает индекс стабильности потребления группы: Определяются все интервалы (в количестве дней) между
-- транзакциями клиента, содержащими анализируемую группу. Из значения каждого интервала вычитается среднее
-- количество дней между транзакциями с анализируемой группой. Получившееся значение для каждого интервала делится
-- на среднее количество дней между транзакциями с анализируемой группой. Вычисляется среднее значение.
CREATE OR REPLACE FUNCTION get_group_stability_index(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    t_group_frequency numeric := (SELECT group_frequency FROM periods
                            WHERE customer_id = t_customer_id AND group_id = t_group_id);
BEGIN
RETURN (
    WITH calc_interval as (
        SELECT
        coalesce(get_diff_between_date_in_days(ph.transaction_datetime,
            lag(ph.transaction_datetime) OVER (ORDER BY ph.transaction_datetime)), 0) as t_interval
        FROM purchase_history ph
        WHERE ph.customer_id = t_customer_id AND ph.group_id = t_group_id)
    SELECT
        coalesce(avg(abs(t_interval - t_group_frequency) / t_group_frequency), 0)
    FROM calc_interval WHERE t_interval > 0
    );
END; $$;
-- SELECT get_group_stability_index(1, 1);


-- функция возвращает фактическую маржу по группе: из суммы, на которую был куплен товар
-- вычитается себестоимость приобретенного товара
CREATE OR REPLACE FUNCTION get_group_margin(
        t_customer_id BIGINT,
        t_group_id BIGINT,
        method INT,
        count INT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    group_margin numeric;
    last_date timestamp := get_date_analysis();
    first_date timestamp := last_date - (INTERVAL '1 day') * count;
BEGIN
    IF method = 1 THEN
        group_margin :=
            (SELECT
                sum(group_summ_paid - group_cost)
            FROM purchase_history
            WHERE customer_id = t_customer_id AND group_id = t_group_id
                AND transaction_datetime BETWEEN first_date AND last_date);
    ELSE
        group_margin :=
            (WITH get_margin as (
                SELECT
                    group_summ_paid - group_cost as margin
                FROM purchase_history
                WHERE customer_id = t_customer_id AND group_id = t_group_id
                LIMIT count)
            SELECT
                sum(margin)
            FROM get_margin);
    END IF;

    RETURN group_margin;
END; $$;
-- SELECT get_group_margin(1, 1, 2, 100);


-- функция возвращает долю транзакций со скидкой у клиента в рамках группы
CREATE OR REPLACE FUNCTION get_group_discount_share(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    group_purchase numeric := (SELECT group_purchase FROM periods WHERE customer_id = t_customer_id AND group_id = t_group_id);
BEGIN
RETURN (
    SELECT
        coalesce(count(*), 0) / group_purchase
    FROM personal_data p
    JOIN cards c ON p.customer_id = c.customer_id AND p.customer_id = t_customer_id
    JOIN transactions t ON c.customer_card_id = t.customer_card_id
    JOIN checks c2 ON t.transaction_id = c2.transaction_id
    JOIN sku s ON s.sku_id = c2.sku_id AND s.group_id = t_group_id AND c2.sku_discount > 0
    );

END; $$;
-- SELECT get_group_discount_share(1, 1);


-- функция возвращает минимальный размер скидки у клиента в рамках группы
CREATE OR REPLACE FUNCTION get_group_minimum_discount(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (SELECT group_min_discount FROM periods WHERE customer_id = t_customer_id AND group_id = t_group_id);
END; $$;
-- SELECT get_group_minimum_discount(1, 1);


-- функция возвращает средний размер скидки у клиента в рамках группы
CREATE OR REPLACE FUNCTION get_group_average_discount(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (SELECT sum(group_summ_paid) / sum(group_summ) FROM purchase_history
                WHERE customer_id = t_customer_id AND group_id = t_group_id);
END; $$;
-- SELECT get_group_average_discount(1, 1);



-- процедура создает таблицу, которая потом используется в представлении Группы
CREATE OR REPLACE FUNCTION create_view_groups(
        method INT DEFAULT 1, --метод расчета актуальной маржи – по периоду (1) или по количеству транзакций (2)
        count INT DEFAULT 2000) -- количество дней от даты формирования анализа,
                                -- за которое необходимо рассчитать маржу
RETURNS table
    (customer_id bigint, -- идентификатор покупателя
    group_id bigint, -- идентификатор группы товаров
    group_affinity_index numeric, -- индекс востребованности группы
    group_churn_rate numeric, -- индекс оттока
    group_stability_index numeric, -- индекс стабильности потребления группы
    group_margin numeric, -- фактическая маржа по группе
    group_discount_share numeric, -- доля транзакций со скидкой в рамках группы,
    group_minimum_discount numeric, -- минимальный размер скидки в рамках группы
    group_average_discount numeric) -- средний размер скидки в рамках группы
LANGUAGE plpgsql
AS $$ BEGIN
-- представление содержит идентификатор покупателя, группы товаров, индекса востребованности группы,
-- индекс оттока, индекс стабильности потребления группы, фактическую маржу по группе,
-- долю транзакций со скидкой в рамках группы, минимальный размер скидки в рамках группы,
-- средний размер скидки в рамках группы
RETURN QUERY
    SELECT
        p.customer_id,
        s.group_id,
        get_group_affinity_index(p.customer_id, s.group_id),
        get_group_churn_rate(p.customer_id, s.group_id),
        get_group_stability_index(p.customer_id, s.group_id),
        get_group_margin(p.customer_id, s.group_id,method, count),
        get_group_discount_share(p.customer_id, s.group_id),
        get_group_minimum_discount(p.customer_id, s.group_id),
        get_group_average_discount(p.customer_id, s.group_id)
    FROM personal_data p
    JOIN cards c ON p.customer_id = c.customer_id
    JOIN transactions t ON c.customer_card_id = t.customer_card_id
    JOIN checks c2 ON t.transaction_id = c2.transaction_id
    JOIN sku s ON c2.sku_id = s.sku_id
    GROUP BY p.customer_id, s.group_id
    ORDER BY p.customer_id, s.group_id;
END $$;


CREATE OR REPLACE VIEW groups as SELECT * FROM create_view_groups();





------------- ТЕСТОВЫЕ ЗАПРОСЫ --------------------

SELECT * FROM groups;

SELECT * FROM groups WHERE customer_id = 1;

SELECT * FROM groups WHERE customer_id = 3;

SELECT * FROM groups WHERE customer_id = 11;

SELECT * FROM groups WHERE group_stability_index = 1;


