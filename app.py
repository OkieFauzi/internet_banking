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
                        phone = row['phone'])
    except Exception as e:
        return jsonify(error=str(e))

@app.route('/user/all', methods = ['GET'])
def get_all_users():
    all = []
    with engine.connect() as connection:
        query = text("SELECT * from public.user")
        result = connection.execute(query)
        for row in result:
            all.append({'user_id' : row['user_id'], 'prefix' : row['prefix'], 'first_name' : row['first_name'], 
                        'last_name' : row['last_name'], 'address' : row['address'], 'job' : row['job'],
                        'source_of_income' : row['source_of_income'], 'phone' : row['phone'] }) 
        return jsonify(all)



if __name__ == "__main__":
    app.run(debug=True)