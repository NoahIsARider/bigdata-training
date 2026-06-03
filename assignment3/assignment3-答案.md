# assignment3 答案

## 案例一、区域人群流量分析

### 1. 解题思路

已知区域 `AH100501000008` 在相邻两个 5 分钟时刻的人群位图分别为：

- A 时刻：`2023-02-03 11:55:00`，对应时间戳 `1675396500000`
- B 时刻：`2023-02-03 12:00:00`，对应时间戳 `1675396800000`

业务口径如下：

- 流入 = `B - A`，即 B 时刻在区域内、A 时刻不在区域内的人
- 保持 = `A ∩ B`，即 A、B 两个时刻都在区域内的人
- 流出 = `A - B`，即 A 时刻在区域内、B 时刻不在区域内的人

根据题目给定数据：

- A = `[1,2,3,4,5,6,7,8,79856]`
- B = `[1,2,3,4,5,6,7,8,9,79856]`

所以：

- 流入 = `[9]`
- 保持 = `[1,2,3,4,5,6,7,8,79856]`
- 流出 = `[]`

对应人数结果：

- 流入人数：`1`
- 保持人数：`9`
- 流出人数：`0`

### 2. 解答

流入：

```sql
WITH
    a AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396500000
    ),
    b AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396800000
    )
SELECT
    bitmapToArray(bitmapAndnot((SELECT bm FROM b), (SELECT bm FROM a))) AS inflow_user_ids,
    bitmapCardinality(bitmapAndnot((SELECT bm FROM b), (SELECT bm FROM a))) AS inflow_cnt;
```

保持：

```sql
WITH
    a AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396500000
    ),
    b AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396800000
    )
SELECT
    bitmapToArray(bitmapAnd((SELECT bm FROM a), (SELECT bm FROM b))) AS keep_user_ids,
    bitmapCardinality(bitmapAnd((SELECT bm FROM a), (SELECT bm FROM b))) AS keep_cnt;
```

流出：

```sql
WITH
    a AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396500000
    ),
    b AS
    (
        SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
        FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
        WHERE REGION_ID = 'AH100501000008'
          AND `5MIN` = 1675396800000
    )
SELECT
    bitmapToArray(bitmapAndnot((SELECT bm FROM a), (SELECT bm FROM b))) AS outflow_user_ids,
    bitmapCardinality(bitmapAndnot((SELECT bm FROM a), (SELECT bm FROM b))) AS outflow_cnt;
```

## 案例二、区域人群画像分析

### 1. 解题思路

题目要求分析 `2023-02-03 11:55:00` 至 `2023-02-03 12:05:00` 的区域画像统计结果，因此先取该区域在这 3 个 5 分钟切片上的区域人群并集。

时间范围内区域位图为：

- `11:55:00` -> `[1,2,3,4,5,6,7,8,79856]`
- `12:00:00` -> `[1,2,3,4,5,6,7,8,9,79856]`
- `12:05:00` -> `[1,2,5,6,7,8,9,79856]`

并集结果为：

- 区域总人群 = `[1,2,3,4,5,6,7,8,9,79856]`
- 总人数 = `10`

再与画像表中的每个画像值位图做交集统计：

- 画像值 `1` -> `[1,2,3]`，人数 `3`
- 画像值 `27` -> `[4,5,6]`，人数 `3`
- 画像值 `48` -> `[7,8,9,79856]`，人数 `4`

若按占比计算：

- `1` 占比 `30%`
- `27` 占比 `30%`
- `48` 占比 `40%`

### 2. 解答

```sql
WITH region_bitmap AS
(
    SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
    FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
    WHERE REGION_ID = 'AH100501000008'
      AND `5MIN` BETWEEN 1675396500000 AND 1675397100000
)
SELECT
    PORTRAIT_ID,
    PORTRAIT_VALUE,
    bitmapCardinality(bitmapAnd((SELECT bm FROM region_bitmap), groupBitmapOrState(PORTRAIT_BITMAP))) AS user_cnt,
    round(
        bitmapCardinality(bitmapAnd((SELECT bm FROM region_bitmap), groupBitmapOrState(PORTRAIT_BITMAP)))
        / bitmapCardinality((SELECT bm FROM region_bitmap)),
        2
    ) AS ratio
FROM TA_PORTRAIT_IMSI_BITMAP
GROUP BY PORTRAIT_ID, PORTRAIT_VALUE
ORDER BY PORTRAIT_ID, PORTRAIT_VALUE;
```

查询结果应为：

| PORTRAIT_ID | PORTRAIT_VALUE | user_cnt | ratio |
| --- | --- | --- | --- |
| 14 | 1 | 3 | 0.30 |
| 14 | 27 | 3 | 0.30 |
| 14 | 48 | 4 | 0.40 |

## 案例三、区域人群手机号明细

### 1. 解题思路

本题仍然先求出 `2023-02-03 11:55:00` 至 `2023-02-03 12:05:00` 的区域总人群位图并集，然后将位图转成用户索引数组，再与 `T_DIM_D_USER_INDEX` 表按 `BITMAP_ID` 关联，即可得到手机号明细。

区域总人群并集为：

- `[1,2,3,4,5,6,7,8,9,79856]`

由于题目示例数据中 `MSISDN` 与 `IMSI` 写成同值，因此最终会返回这 10 条明细。

### 2. 解答

```sql
WITH region_bitmap AS
(
    SELECT groupBitmapOrState(IMSI_INDEXES) AS bm
    FROM TA_5MIN_REGION_RESIDENT_IMSI_BITMAP
    WHERE REGION_ID = 'AH100501000008'
      AND `5MIN` BETWEEN 1675396500000 AND 1675397100000
),
region_user_ids AS
(
    SELECT arrayJoin(bitmapToArray((SELECT bm FROM region_bitmap))) AS BITMAP_ID
)
SELECT
    u.BITMAP_ID,
    u.IMSI,
    u.MSISDN
FROM T_DIM_D_USER_INDEX AS u
INNER JOIN region_user_ids AS r
    ON u.BITMAP_ID = r.BITMAP_ID
ORDER BY u.BITMAP_ID;
```

查询结果应包含以下 `BITMAP_ID`：

`1, 2, 3, 4, 5, 6, 7, 8, 9, 79856`

## 补充说明

如果你想把时间直接写成日期字符串而不是手写时间戳，可以在 ClickHouse 中写成下面这种形式：

```sql
toUnixTimestamp(toDateTime('2023-02-03 12:00:00', 'Asia/Shanghai')) * 1000
```

这样更方便阅读，也更符合题目里“时间需要转换成时间戳”的要求。

另外，以上 SQL 已在本地实际连接的 ClickHouse `26.5.1.882` 中完成验证。对 `AggregateFunction(groupBitmap, UInt32)` 类型字段做位图运算时，实测应使用 `groupBitmapOrState(...)` 来得到可继续参与 `bitmapAnd`、`bitmapAndnot`、`bitmapToArray` 的位图对象。
