from clickhouse import Clickhouse
from dhis2 import Dhis2
d2 = Dhis2()
ch = Clickhouse(True)
art_program_table_name = "artprogram"
columns = ("tei", "psi", "executiondate", "UXuxGZw3bxz",
           "SSKW9i9e5uu", "oHRN2HsfkHW", "HpbDuBPxhM8", "BR1fUe7Nx8V")
ch.init_table(art_program_table_name, columns)
d2.populate_art_program_table(ch, art_program_table_name, columns)
# hello
