def query_gtOTYxbpf7U(ch):
    table_name = "warehouse.artprogram"
    query = f"""
        with condition1 as (
            select distinct(tei) from {table_name}
            where ("UXuxGZw3bxz" != '0'
                    and "UXuxGZw3bxz" != '1'
                    and "UXuxGZw3bxz" != '3'
                    and "UXuxGZw3bxz" != '4'
            )
        ),
        condition2 as (
            select distinct(tei) from {table_name}
            where ("UXuxGZw3bxz" = '0'
                    or "UXuxGZw3bxz" = '1'
                    or "UXuxGZw3bxz" = '3'
                    or "UXuxGZw3bxz" = '4'
            )
            and "SSKW9i9e5uu" > '2023-01-31'

        ),
        condition3 as (
            select distinct(tei) from {table_name}
            where (
                enrollmentdate <= '2023-01-31'
                and date_diff('day', toDate(executiondate),
                          toDate('2023-01-31')) >= 0
                and date_diff('day', toDate(executiondate), toDate('2023-01-31')) <= 365
                and toFloat32("oHRN2HsfkHW") >= 0
                and toFloat32("oHRN2HsfkHW") < 1000
            )
            and toFloat32("HpbDuBPxhM8") >= 0
            and toFloat32("HpbDuBPxhM8") <= 14
            and "BR1fUe7Nx8V" = 'Female'
        )
        select count(*) from {table_name}
        where (tei in condition1
        or tei in condition2)
        and tei in condition3
    """
    print(query)
    result = ch.query(query)

    return result
