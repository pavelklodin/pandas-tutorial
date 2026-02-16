# Задача №3 — Sales Enrichment and Profitability Analysis

## Условие

Вам предоставлены два CSV-файла:

1️⃣ `sales.csv`

Содержит записи о продажах.

| column     | description            |
|------------|------------------------|
| order_id   | уникальный ID заказа   |
| product_id | идентификатор продукта |
| region     | регион продажи         |
| quantity   | проданное количество   |
| unit_price | цена за единицу        |

2️⃣ `products.csv`

Справочник продуктов.

| column       | description            |
|--------------|------------------------|
| product_id   | идентификатор продукта |
| product_name | имя продукта           |
| category     | категория              |
| unit_cost    | себестоимость единицы  |

## Требуется реализовать скрипт, который

### Шаг 1 — загрузка

- Загружает оба файла
- Проверяет наличие обязательных колонок
- Прерывает выполнение с понятной ошибкой при отсутствии

### Шаг 2 — объединение данных

- Выполнить left join:
- `sales LEFT JOIN products ON product_id`

### Шаг 3 — обработка edge cases

1️⃣ Если product отсутствует в справочнике
- сохранить запись
- product_name = "UNKNOWN"
- category = "UNKNOWN"
- unit_cost = 0

2️⃣ Если quantity <= 0
- запись должна быть отброшена

3️⃣ Если unit_price < 0
- запись должна быть отброшена

### Шаг 4 — вычисляемые поля

Добавить столбцы:
```
revenue = quantity * unit_price
cost    = quantity * unit_cost
profit  = revenue - cost
```

### Шаг 5 — агрегация

1. Сгруппировать по:
   - region,
   - category
2. Рассчитать:
   | metric          | definition                   |
   |-----------------|------------------------------|
   | `orders_count`  | число заказов                |
   | `total_revenue` | сумма `revenue`              |
   | `total_cost`    | сумма `cost`                 |
   | `total_profit`  | сумма `profit`               |
   | `profit_margin` | total`_profit/total_revenue` |
3. Округлить денежные значения до 2 знаков.

### Шаг 6 — глобальный итог

Добавить строку:
```
region="ALL"
category="ALL"
```
с агрегатами по всему датасету.

### Шаг 7 - сортировка результатов

```
region ASC
category ASC
```

### Шаг 8 — вывод

Сохранить результат в _CSV_.


## Входные данные

`sales.csv`
```
order_id,product_id,region,quantity,unit_price
1,101,EU,10,20
2,102,EU,5,15
3,103,US,8,30
4,999,US,4,25
5,101,EU,-3,20
6,102,APAC,7,-10
7,103,APAC,6,30
8,101,US,3,20
```

`products.csv`
```
product_id,product_name,category,unit_cost
101,Widget A,Hardware,8
102,Widget B,Hardware,6
103,Service C,Services,12
```

## Golden Output (для проверки)

```
region,category,orders_count,total_revenue,total_cost,total_profit,profit_margin
ALL,ALL,6,695.00,272.00,423.00,0.61
APAC,Services,1,180.00,72.00,108.00,0.60
EU,Hardware,2,275.00,110.00,165.00,0.60
US,Hardware,1,60.00,24.00,36.00,0.60
US,Services,1,240.00,96.00,144.00,0.60
US,UNKNOWN,1,100.00,0.00,100.00,1.00
```

## Почему эта задача важна (интервью-контекст)

Она проверяет сразу несколько ключевых навыков:
- pandas merge/join семантика
- обработка грязных данных
- derived metrics pipeline
- multi-metric aggregation
- численная устойчивость (деление)
- читаемость архитектуры кода
- error handling

Это уже уровень typical take-home задания.