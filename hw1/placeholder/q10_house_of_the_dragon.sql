WITH T AS (
    SELECT
        TMP.title,
        ROW_NUMBER() OVER (
            ORDER BY
                TMP.title
        ) AS row_number,
        ROW_NUMBER() OVER (
            ORDER BY
                TMP.title
        ) - 1 AS parent_row_number
    FROM
        (
            SELECT
                DISTINCT akas.title
            FROM
                titles
                JOIN akas ON titles.title_id = akas.title_id
            WHERE
                original_title = 'House of the Dragon'
                AND type = 'tvSeries'
            ORDER BY
                title
        ) AS TMP
),
RECV AS (
    SELECT
        row_number,
        parent_row_number,
        title
    FROM
        T
    WHERE
        row_number = 1
    UNION
    ALL
    SELECT
        T.row_number,
        T.parent_row_number,
        Recv.title || ', ' || T.title AS title
    FROM
        Recv
        JOIN T ON T.parent_row_number = Recv.row_number
)
SELECT
    title
FROM
    RECV
order by
    row_number  desc
limit
    1;