SELECT
    name,
    age
FROM
    (
        SELECT
            name,
            (died - born) AS age
        FROM
            people
        WHERE
            born >= 1900
            AND died IS NOT NULL
        UNION
        SELECT
            name,
            (2022 - born) AS age
        FROM
            people
        WHERE
            born >= 1900
            AND died IS NULL
    )
ORDER BY
    age desc, name asc
LIMIT
    20;