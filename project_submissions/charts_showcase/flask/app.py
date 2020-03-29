from data_fetch import db_read
from flask import Flask, Markup, render_template

app = Flask(__name__)

labels = []

values = []

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]


df = db_read(
    "select customers.LastName,invoices.Total from invoices INNER JOIN customers "
    "ON invoices.CustomerId=customers.CustomerId",
    50)
df_list = df.values.tolist()

print(df_list)

for row in df_list:
    labels.append(row[0])
    values.append(row[1])

print(labels)
print(values)


@app.route('/bar')
def bar():
    bar_labels = labels
    bar_values = values
    return render_template('bar_chart.html', title='Total invoices', max=30, labels=bar_labels,
                           values=bar_values)


@app.route('/customer_invoices')
def customer_invoices():
    #create dropdown of customers
    #user selects dropdown
    #user clicks submit
    #graph changes to customer invoice by month
    
    #use python Altair
    #run sql dinamically
    pass

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=8080)
