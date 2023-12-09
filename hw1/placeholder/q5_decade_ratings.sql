select
    decade || "s",
    avg_rating,
    top_rating,
    min_rating,
    num_release
from
    (
        select
            titles.premiered / 10 * 10 as decade,
            round(avg(ratings.rating), 2) as avg_rating,
            max(ratings.rating) as top_rating,
            min(ratings.rating) as min_rating,
            count(titles.title_id) as num_release
        from
            ratings,
            titles
        where
            ratings.title_id = titles.title_id
            and titles.premiered is not NULL
        group by
            decade
    ) as T
ORDER BY
    avg_rating desc,
    decade ASC