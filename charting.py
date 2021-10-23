from bokeh.plotting import figure
from bokeh.io import show, output_file
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.models import DatetimeTickFormatter, NumeralTickFormatter
from bokeh.transform import linear_cmap
from bokeh.palettes import brewer
from bokeh.models import Label
import numpy as np
import pandas as pd
from datetime import datetime
from amz_transactions import Cashflows


# Functions
def style(p):
    p.title.align = 'center'
    p.title.text_font_size = '18pt'
    p.xaxis.axis_label_text_font_size = '12pt'
    p.xaxis.major_label_text_font_size = '8pt'
    p.yaxis.axis_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_size = '8pt'
    return p


# Get DataFrame of data
raw_data, hist_bin = Cashflows().get_cashflows()

# Use only OPEX
raw_data = raw_data[raw_data['expenditure']=='opex']

# Generate histogram of cashflows
date_epochs = raw_data['date'].values.astype(np.int64) // 10 ** 9
hist_bin_epoch = [int(tim.timestamp()) for tim in hist_bin]
# Histogram functions determines histogram and edges based on provided data
# Since we want to divide by date, provide epoch monthly dates as bins
# Primary data provided will be transaction_dates as epoch as bins will be divided over it
# Histogram will be counted up by weights -> raw_data['amounts']
arr_hist, edges = np.histogram(date_epochs, bins=hist_bin_epoch, weights=raw_data['amount'].values)
hist_cashflows = pd.DataFrame({'amount': arr_hist, 'left': edges[:-1], 'right': edges[1:]})

# Massage dates of df for graphing
hist_cashflows['left_dates'] = [datetime.fromtimestamp(fro) for fro in hist_cashflows['left']]
hist_cashflows['middle_dates'] = hist_cashflows['right']-hist_cashflows['left']
hist_cashflows['right_dates'] = [datetime.fromtimestamp(fro) for fro in hist_cashflows['right']]
hist_cashflows['left_dates_desc'] = [fro.strftime('%b %Y') for fro in hist_cashflows['left_dates']]
hist_cashflows['middle_dates_desc'] = [fro.strftime('%b %Y') for fro in hist_cashflows['left_dates']]
hist_cashflows['right_dates_desc'] = [fro.strftime('%b %Y') for fro in hist_cashflows['right_dates']]
hist_cashflows['amount_desc'] = [f'$ {amount:.2f}' for amount in hist_cashflows['amount']]
hist_cashflows['cum_amount'] = hist_cashflows['amount'].cumsum()
hist_cashflows['cum_amount_desc'] = [f'$ {amount:.2f}' for amount in hist_cashflows['cum_amount']]

# Convert dataframe to column data source
src = ColumnDataSource(hist_cashflows)

# Create the blank plot
p = figure(plot_height = 600, plot_width = 600,
           title = 'Expenses of Tangeeble Products',
           x_axis_label = 'Date',
           x_axis_type="datetime",
           y_axis_label = 'Cashflow')

# Add color mapper
cus_palette = tuple(reversed(brewer['RdYlGn'][5]))
colormapper = linear_cmap(field_name='amount', palette=cus_palette, low=-100, high=100)


# Add a quad glyph
p.quad(source=src,bottom=0, top='amount',
       left='left_dates', right='right_dates',
       fill_color=colormapper, line_color='black',fill_alpha = 0.75)

hover = HoverTool(tooltips = [('Month','@left_dates_desc'),
                              ('Cashflow', '@amount_desc'),
                              ('Cumulative', '@cum_amount_desc')])

last_date = pd.to_datetime(raw_data['date'].values[-1])
last_date_lbl = Label(x=hist_cashflows['right_dates'].values[-1],
                      y=hist_cashflows['amount'].values[-1],
                      text=f'Last Date: {last_date}')

p.add_tools(hover)
p.add_layout(last_date_lbl)

p.line(source=src,x='right_dates',y='cum_amount', line_color='black', line_width=5)
p.circle(source=src,x='right_dates',y='cum_amount', size=20, color="navy", alpha=0.5)

p = style(p)

# format axes ticks
p.yaxis[0].formatter = NumeralTickFormatter(format="$0.00")
p.xaxis[0].formatter = DatetimeTickFormatter(months="%b %Y")

# Show the plot
output_file('charting.html')
show(p)
