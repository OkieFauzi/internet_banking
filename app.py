from flask import Flask, json, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.sql import text

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

if __name__ == "__main__":
    app.run(debug=True)