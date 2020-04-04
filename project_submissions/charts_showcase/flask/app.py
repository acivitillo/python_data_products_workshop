from data_fetch import db_read
from flask import Flask, render_template
import altair
import pandas

app = Flask(__name__)


def split_into_label_values(data_set):
    lab = []
    val = []
    for row in data_set:
        lab.append(row[0])
        val.append(row[1])
    return lab, val


def get_table(table_name, max_rows):
    sql_statement = "SELECT * from " + table_name
    return db_read(sql_statement, max_rows)


def shorten_dates(dates_tab):
    for i in range(len(dates_tab)):
        dates_tab[i] = dates_tab[i][0:10]
    return dates_tab


@app.route('/customers')
def customers():
    df = get_table("customers", 100)

    return render_template('table.html',  title='Customers', tables=[df.to_html(classes='data')],
                           titles=df.columns.values)


@app.route('/invoices')
def invoices():
    df = get_table("invoices", 100)

    return render_template('table.html',  title='Invoices', tables=[df.to_html(classes='data')],
                           titles=df.columns.values)


@app.route('/customer_invoices')
def customer_invoices():
    df = db_read(
        "select invoices.InvoiceDate,invoices.Total from invoices INNER JOIN customers "
        "ON invoices.CustomerId=customers.CustomerId WHERE invoices.CustomerId=2",
        50)

    df_list = df.values.tolist()
    print(df_list)
    labels, values = split_into_label_values(df_list)
    labels = shorten_dates(labels)
    # print(values)
    # print(labels)

    data = pandas.DataFrame({
        'Customer': labels,
        'Invoices': values
    })

    chart = altair.Chart(data).mark_bar().encode(
        x='Customer',
        y='Invoices'
    )
    chart.save('templates/customer_invoices.html')

    return render_template("customer_invoices.html")

    # create dropdown of customers
    # user selects dropdown
    # user clicks submit
    # graph changes to customer invoice by month
    #
    # + use python Altair
    # run sql dynamically


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
