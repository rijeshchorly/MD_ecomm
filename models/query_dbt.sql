SELECT CAST(CAST(DATE_TRUNC('MONTH', "last_session_at") AS TIMESTAMP) AS TIMESTAMP) AS "last_session_at[month]",
    "lifetime_gross_profit"
FROM {{ref('int_user_sessions')}} AS "intermediate_prod__int_user_sessions"
GROUP BY 1, 2
