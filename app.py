from flask import Flask, json, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, timezone

app = Flask(__name__)
conn_str = 'postgresql://postgres:parallelepiped@localhost:5432/internet_banking'
engine = create_engine(conn_str, echo=False)

#user management

@app.route('/user/registration', methods = ['POST'])
def user_registration():
    body = request.json
    tbprefix = body.get('prefix')
    tbfirst_name = body.get('first_name')
    tblast_name = body.get('last_name')
    tbaddress = body.get('address')
    tbjob = body.get('job')
    tbsource_of_income = body.get('source_of_income')
    tbphone = body.get('phone')
    try:
        with engine.connect() as connection:
            qry = text("INSERT INTO public.user(prefix, first_name, last_name, address, job, source_of_income, phone) VALUES (:prefix, :first_name, :last_name, :address, :job, :source_of_income, :phone)")
            connection.execute(qry, prefix = tbprefix, first_name = tbfirst_name, last_name = tblast_name, address = tbaddress, job = tbjob, source_of_income = tbsource_of_income, phone = tbphone)
            current_insert = text("select * from public.user where user_id = (select max(user_id) from public.user)")
            result = connection.execute(current_insert)
            for row in result:
                return jsonify(user_id = row['user_id'], prefix = row['prefix'], first_name = row['first_name'], 
                    last_name = row['last_name'], address = row['address'], job = row['job'], source_of_income = row['source_of_income'],
                    phone = row['phone'], last_update = row['last_update'])
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/user/all', methods = ['GET'])
def get_all_users():
    all = []
    try:
        with engine.connect() as connection:
            query = text("SELECT * from public.user")
            result = connection.execute(query)
            for row in result:
                all.append({'user_id' : row['user_id'], 'prefix' : row['prefix'], 'first_name' : row['first_name'], 
                    'last_name' : row['last_name'], 'address' : row['address'], 'job' : row['job'],
                    'source_of_income' : row['source_of_income'], 'phone' : row['phone'], 'last_update' : row['last_update'] }) 
            return jsonify(all)
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/user/<_id>', methods = ['DELETE'])
def delete_user_by_id(_id):
    try:
        with engine.connect() as connection:
            query = text("delete from public.user where user_id = :user_id")
            connection.execute(query, user_id = _id)
            return jsonify(deleted_user_id = _id)
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/user/<_id>', methods = ['PUT'])
def update_user_by_id(_id):
    body = request.json
    tbprefix = body.get('prefix')
    tbfirst_name = body.get('first_name')
    tblast_name = body.get('last_name')
    tbaddress = body.get('address')
    tbjob = body.get('job')
    tbsource_of_income = body.get('source_of_income')
    tbphone = body.get('phone')
    with engine.connect() as connection:
        query = text("update public.user set prefix = :prefix, first_name = :first_name, last_name = :last_name,\
		    address = :address, job = :job, source_of_income = :source_of_income, phone = :phone,\
            last_update = current_timestamp where user_id = :user_id")
        connection.execute(query, user_id = _id, prefix = tbprefix, first_name = tbfirst_name, last_name = tblast_name, 
            address = tbaddress, job = tbjob, source_of_income = tbsource_of_income, phone = tbphone)
        view = text("select * from public.user where user_id = :user_id")
        result = connection.execute(view, user_id = _id)
        for row in result:
            return jsonify(user_id = row['user_id'], prefix = row['prefix'], first_name = row['first_name'], 
                    last_name = row['last_name'], address = row['address'], job = row['job'], source_of_income = row['source_of_income'],
                    phone = row['phone'], last_update = row['last_update'])

#branch management

@app.route('/branch/new', methods = ['POST'])
def create_new_branch():
    body = request.json
    tbbranch_name = body.get('branch_name')
    tbcity = body.get('city')
    tbaddress = body.get('address')
    try:
        with engine.connect() as connection:
            qry = text("INSERT INTO branch(branch_name, city, address) VALUES (:branch_name, :city, :address)")
            connection.execute(qry, branch_name = tbbranch_name, city = tbcity, address = tbaddress)
            current_insert = text("select * from branch where branch_id = (select max(branch_id) from branch)")
            result = connection.execute(current_insert)
            for row in result:
                return jsonify(branch_id = row['branch_id'], branch_name = row['branch_name'], city = row['city'],
                    address = row['address'], last_update = row['last_update'])
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/branch/all', methods = ['GET'])
def get_all_branch():
    all = []
    try:
        with engine.connect() as connection:
            query = text("SELECT * from branch")
            result = connection.execute(query)
            for row in result:
                all.append({'branch_id' : row['branch_id'], 'branch_name' : row['branch_name'], 'city' : row['city'],
                'address' : row['address'], 'last_update' : row['last_update']}) 
            return jsonify(all)
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/branch/<_id>', methods = ['DELETE'])
def delete_branch_by_id(_id):
    try:
        with engine.connect() as connection:
            query = text("delete from branch where branch_id = :branch_id")
            connection.execute(query, branch_id = _id)
            return jsonify(deleted_branch_id = _id)
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/branch/<_id>', methods = ['PUT'])
def update_branch_by_id(_id):
    body = request.json
    tbbranch_name = body.get('branch_name')
    tbcity = body.get('city')
    tbaddress = body.get('address')
    with engine.connect() as connection:
        query = text("update branch set branch_name = :branch_name, city = :city, address = :address,\
            last_update = current_timestamp where branch_id = :branch_id")
        connection.execute(query, branch_id = _id, branch_name = tbbranch_name, city = tbcity, address = tbaddress)
        view = text("select * from branch where branch_id = :branch_id")
        result = connection.execute(view, branch_id = _id)
        for row in result:
            return jsonify(branch_id = row['branch_id'], branch_name = row['branch_name'], city = row['city'],
                    address = row['address'], last_update = row['last_update'])

#Account Management

@app.route('/account', methods = ['POST'])
def create_new_account():
    body = request.json
    tbuser_id = body.get('user_id')
    tbbranch_id = body.get('branch_id')
    tbfirst_deposit = body.get('first_deposit')
    with engine.connect() as connection:
        query = text("INSERT INTO account(user_id, branch_id, balance) VALUES (:user_id, :branch_id, :first_deposit)")
        connection.execute(query, user_id = tbuser_id, branch_id = tbbranch_id, first_deposit = tbfirst_deposit)
        current_insert = text("select * from account where account_id = (select max(account_id) from account)")
        result = connection.execute(current_insert)
        for row in result:
            return jsonify(account_id = row['account_id'], user_id = row['user_id'], branch_id = row['branch_id'],
                balance = row['balance'], status = row['status'], last_update = row['last_update'])

@app.route('/account/<_id>', methods = ['PUT'])
def update_account(_id):
    body = request.json
    tbuser_id = body.get('user_id')
    tbbranch_id = body.get('branch_id')
    tbbalance = body.get('balance')
    tbstatus = body.get('status')
    with engine.connect() as connection:
        query = text("update account set user_id = :user_id, branch_id = :branch_id, balance = :balance,\
            status = :status, last_update = current_timestamp where account_id = :account_id")
        connection.execute(query, account_id = _id, user_id = tbuser_id, branch_id = tbbranch_id,
            balance = tbbalance, status = tbstatus)
        current_update = text("select * from account where account_id = :account_id")
        result = connection.execute(current_update, account_id = _id)
        for row in result:
            return jsonify(account_id = row['account_id'], user_id = row['user_id'], branch_id = row['branch_id'],
                balance = row['balance'], status = row['status'], last_update = row['last_update'])

@app.route('/account/close/<_id>', methods = ['PUT'])
def close_account(_id):
    with engine.connect() as connection:
        query = text("update account set status = 'closed', last_update = current_timestamp where account_id = :account_id")
        connection.execute(query, account_id = _id)
        current_update = text("select * from account where account_id = :account_id")
        result = connection.execute(current_update, account_id = _id)
        for row in result:
            return jsonify(account_id = row['account_id'], user_id = row['user_id'], branch_id = row['branch_id'],
                balance = row['balance'], status = row['status'], last_update = row['last_update'])

#Account Activity

@app.route('/transaction/save/<_id>', methods = ['POST'])
def save(_id):
    body = request.json
    tbamount = body.get('amount')
    with engine.connect() as connection:
        status_query = text("select status from account where account_id = :account_id")
        status = connection.execute(status_query, account_id = _id)
        for row in status:
            if row['status'] == 'closed':
                return jsonify(transaction_status = 'aborted', cause = 'account is closed')
            elif row['status'] == 'active':
                query = text("insert into transaction(account_id, type_of_transaction, amount) values (:account_id, 'save', :amount)")
                connection.execute(query, account_id = _id, amount = tbamount)
                update_account = text("update account set balance = balance + :amount where account_id = :account_id")
                connection.execute(update_account, account_id = _id, amount = tbamount)
                account_info = text("select * from account where account_id = :account_id")
                result = connection.execute(account_info, account_id = _id)
                for row in result:
                    return jsonify(account_id = row['account_id'], user_id = row['user_id'], branch_id = row['branch_id'],
                    balance = row['balance'], status = row['status'], last_update = row['last_update'])

@app.route('/transaction/withdraw/<_id>', methods = ['POST'])
def withdraw(_id):
    body = request.json
    tbamount = body.get('amount')
    with engine.connect() as connection:
        status_query = text("select * from account where account_id = :account_id")
        status = connection.execute(status_query, account_id = _id)
        for row in status:
            if row['status'] == 'closed':
                return jsonify(transaction_status = 'aborted', cause = 'account is closed')
            elif row['status'] == 'active':
                if int(row['balance']) - int(tbamount) < 50000:
                    return jsonify(transaction_status = 'aborted', cause = 'balance is not enough to do transaction')
                else:
                    query = text("insert into transaction(account_id, type_of_transaction, amount) values (:account_id, 'withdraw', :amount)")
                    connection.execute(query, account_id = _id, amount = tbamount)
                    update_account = text("update account set balance = balance - :amount where account_id = :account_id")
                    connection.execute(update_account, account_id = _id, amount = tbamount)
                    account_info = text("select * from account where account_id = :account_id")
                    result = connection.execute(account_info, account_id = _id)
                    for row in result:
                        return jsonify(account_id = row['account_id'], user_id = row['user_id'], branch_id = row['branch_id'],
                        balance = row['balance'], status = row['status'], last_update = row['last_update'])

@app.route('/transaction/history/<_id>', methods = ['GET'])
def get_history_by_id(_id):
    with engine.connect() as connection:
        query = text("select * from transaction where account_id = :account_id")
        result = connection.execute(query, account_id = _id)
        list = []
        for row in result:
            list.append({'transaction_id' : row['transaction_id'], 'account_id' : row['account_id'],\
                'type_of_transaction' : row['type_of_transaction'], 'amount' : row['amount'],\
                'destination' : row['destination'], 'datetime' : row['datetime'] })
        return jsonify(list)

@app.route('/transaction/transfer/<_id>', methods = ['POST'])
def transfer(_id):
    body = request.json
    tbamount = body.get('amount')
    tbdestination = body.get('account_destination')
    with engine.connect() as connection:
        status_query = text("select * from account where account_id = :account_id")
        status = connection.execute(status_query, account_id = _id)
        for row in status:
            if row['status'] == 'closed':
                return jsonify(transaction_status = 'aborted', cause = 'account is closed')
            elif row['status'] == 'active':
                if int(row['balance']) - int(tbamount) < 50000:
                    return jsonify(transaction_status = 'aborted', cause = 'balance is not enough to do transaction')
                else:
                    validate_query = text("select * from account where account_id = :destination")
                    validate = connection.execute(validate_query, destination = tbdestination)
                    for row in validate:
                        if int(row['account_id']) != int(tbdestination):
                            return jsonify(transaction_status = 'aborted', cause = 'destination account is not exist')
                        elif int(row['account_id']) == int(tbdestination) and row['status'] == 'closed':
                            return jsonify(transaction_status = 'aborted', cause = 'destination account is closed')
                        elif int(row['account_id']) == int(tbdestination) and row['status'] == 'active':
                            #update_sender
                            transfer_query = text("insert into transaction(account_id, type_of_transaction, amount, destination_or_sender) values (:account_id, 'transfer', :amount, :destination)")
                            connection.execute(transfer_query, account_id = _id, amount = tbamount, destination = tbdestination)
                            update_sender = text("update account set balance = balance - :amount where account_id = :account_id")
                            connection.execute(update_sender, account_id = _id, amount = tbamount)
                            #update_receiver
                            receiving_query = text("insert into transaction(account_id, type_of_transaction, amount, destination_or_sender) values (:account_id, 'receiving', :amount, :sender)")
                            connection.execute(receiving_query, account_id = tbdestination, amount = tbamount, sender = _id)
                            update_receiver = text("update account set balance = balance + :amount where account_id = :account_id")
                            connection.execute(update_receiver, account_id = tbdestination, amount = tbamount)
                            #view_transfer_status
                            transaction_info = text("select * from transaction where account_id = :account_id and transaction_id = \
                                (select max(transaction_id) from transaction where account_id = :account_id)")
                            result = connection.execute(transaction_info, account_id = _id)
                            for row in result:
                                return jsonify(transaction_id = row['transaction_id'], account_id = row['account_id'],
                                    type_of_transaction = row['type_of_transaction'], amount = row['amount'], 
                                    destination = row['destination_or_sender'], datetime = row['datetime'],
                                    status = 'success' )

#Reporting

@app.route('/report/total/<_id>', methods = ['GET'])
def get_total_account_by_branch_id(_id):
    with engine.connect() as connection:
        query = text("select * from (select branch_id, count(account_id) as number_of_accounts, \
            sum(balance) as total_balance from account where branch_id = :branch_id group by branch_id) as total cross join \
            (select count(user_id) as number_of_users from(select distinct user_id\
            from account where branch_id = :branch_id) as users) as num_users")
        result = connection.execute(query, branch_id = _id)
        for row in result:
            return jsonify(branch_id = row['branch_id'], number_of_accounts = row['number_of_accounts'], total_balance = str(row['total_balance']),
                number_of_users = row['number_of_users'])

@app.route('/report/debit_credit/<_id>', methods = ['GET'])
def get_debit_credit_by_branch_id(_id):
    body = request.json
    tbstart_date = body.get('start_date')
    tbend_date = body.get('end_date')
    with engine.connect() as connection:
        query = text("select * from (select branch_id, sum(amount) as total_debit from transaction inner join\
            account on transaction.account_id = account.account_id where branch_id = :branch_id and\
            type_of_transaction = 'save' and datetime > :start_date and datetime < :end_date group by\
            branch_id) as debit cross join (select sum(amount) as total_credit from transaction inner join\
            account on transaction.account_id = account.account_id where branch_id = :branch_id and\
            type_of_transaction = 'withdraw' and datetime > :start_date and datetime < :end_date) as credit")
        result = connection.execute(query, branch_id = _id, start_date = tbstart_date, end_date = tbend_date)
        for row in result:
            return jsonify(branch_id = row['branch_id'], total_debit = str(row['total_debit']), total_credit = str(row['total_credit']))

@app.route('/report/dormant_account', methods = ['GET'])
def get_dormant_account():
    with engine.connect() as connection:
        all = []
        max_account_query = text("select max(account_id) as max_account_id from transaction")
        max_account = connection.execute(max_account_query)
        for row in max_account:
            tbaccount_id = 1
            while tbaccount_id <= row['max_account_id']:
                account_table_query = text("select * from transaction where account_id = :account_id order by datetime")
                account_table = connection.execute(account_table_query, account_id = tbaccount_id)
                time1 = datetime.now(timezone.utc)
                for baris in account_table:
                    time_span = time1 - baris['datetime']
                    total_time_span = int(time_span.total_seconds()/86400)
                    if total_time_span > 90 and time1 == datetime.now(timezone.utc):
                        all.append({'account_id' : baris['account_id'], 'next_transaction' : 'none',\
                            'last_transaction' : baris['datetime'],\
                            'days_of_dormant_period' : total_time_span})
                    elif total_time_span > 90 and time1 != datetime.now(timezone.utc):
                        all.append({'account_id' : baris['account_id'], 'next_transaction' : time1,\
                            'last_transaction' : baris['datetime'],\
                            'days_of_dormant_period' : total_time_span})
                    time1 = baris['datetime']
                tbaccount_id += 1
            return jsonify(all)
                        





            

if __name__ == "__main__":
    app.run(debug=True)