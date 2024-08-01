from clickhouse import Clickhouse
from calculations import calculations
ch = Clickhouse(False)
result = calculations.query_gtOTYxbpf7U(ch)
print(result)
