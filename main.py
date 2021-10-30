from amz_transactions import Transactions, Cashflows
from amz_charting import generate_cashflow_graph

# Cell to insert transactions
with Transactions(configfile='psql_businessfinances.ini') as db:
    db.insert_transactions()

# Cell to generate graph
generate_cashflow_graph('all')
generate_cashflow_graph('capex')
generate_cashflow_graph('opex')