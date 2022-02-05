from amz_transactions import Transactions, Cashflows
from amz_charting import generate_cashflow_graph

# Cell to insert transactions
with Transactions(configfile='psql_businessfinances.ini') as db:
    # Insert transactions
    db.insert_transactions()

    # Check for missing and raise AssertionError if there are any missing
    missing = db.check_missing_settlements()
    assert len(missing) == 0, f"Missing Transactions:\n {missing}"

# Cell to generate graph
generate_cashflow_graph('all')
generate_cashflow_graph('capex')
generate_cashflow_graph('opex')