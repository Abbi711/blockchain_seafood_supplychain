import bitarray 
from hashlib import sha256
import json
import time
import os
import math
from flask import Flask, request, redirect,render_template,flash
import requests
import schedule
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime,timedelta

class Block:
    def __init__(self, index, trans, timestamp, prev_hash, nonce=0):
        self.index = index
        self.trans = trans
        self.timestamp = timestamp
        self.prev_hash = prev_hash
        self.nonce = nonce

    def calculate_hash(self):
        block_text = json.dumps(self.__dict__, sort_keys=True)
        return sha256(block_text.encode()).hexdigest()


class Blockchain:
    difficulty = 2

    def __init__(self):
        self.unconf_trans = []
        self.chain = []

    def build_genesis_block(self):
        
        gen_block = Block(0, [], 0, "0")
        gen_block.hash = gen_block.calculate_hash()
        self.chain.append(gen_block)

    @property
    def final_block(self):
        return self.chain[-1]

    def add_new_block(self, block, proof):
       
       prev_hash = self.last_block.hash

        if prev_hash != block.prev_hash:
            return False

        if not Blockchain.is_valid_proof(block, proof):
            return False

        block.hash = proof
        self.chain.append(block)
        return True

    @staticmethod
    def proof_of_work(block):
        
        block.nonce = 0

        calculated_hash = block.calculate_hash()
        while not calculated_hash.startswith('0' * Blockchain.difficulty):
            block.nonce += 1
            calculated_hash = block.calculate_hash()

        return calculated_hash

    def add_new_trans(self, trans):
        self.unconf_trans.append(trans)

    @classmethod
    def is_valid_proof(cls, block, block_hash):
                return (block_hash.startswith('0' * Blockchain.difficulty) and
                block_hash == block.calculate_hash())

    @classmethod
    def check_chain_validity(cls, chain):
        result = True
        prev_hash = "0"

        for block in chain:
            block_hash = block.hash
            delattr(block, "hash")

            if not cls.is_valid_proof(block, block.hash) or \
                    prev_hash != block.prev_hash:
                result = False
                break

            block.hash, prev_hash = block_hash, block_hash

        return result

    def mine(self):
    
        if not self.unconf_trans:
            return False

        final_block = self.final_block

        new_block = Block(index=final_block.index + 1,
                          trans=self.unconf_trans,
                          timestamp=time.time(),
                          prev_hash=final_block.hash)

        proof = self.proof_of_work(new_block)
        
        fh = open("details.txt", "a")
        i=0
        data = {}
        data['NewBlock'] = []
        data['NewBlock'].append({
        'Index':new_block.index,
        'Trans': new_block.trans,
        'Timestamp': new_block.timestamp, 
        'prev_hash':new_block.prev_hash,
        'proof':proof,
	})
	
        with open('details.txt', 'a') as outfile:
             json.dump(data, outfile)
       
    
        self.add_new_block(new_block, proof)

        self.unconf_trans = []

        return True


project_root = os.path.dirname(__file__)
template_path = os.path.join(project_root, 'app/templates')

app = Flask(__name__, template_folder=template_path)
app.secret_key = b'rjnkcfnvfknv'

blockchain = Blockchain()
blockchain.build_genesis_block()

peers = set()

conn = pymysql.connect("localhost","root","password","project")

global uname
@app.route('/authenticate', methods=['GET', 'POST'])
def authenticate():
    global uname
    uname= request.form["uname"]
    pwd1 = request.form["pwd"]
    pwd=sha256(pwd1.encode()).hexdigest()
    
    utype = request.form["s1"]        
    cursor = conn.cursor()
    try:
          sql="SELECT password FROM login where username=%s AND type=%s;"
          cursor.execute(sql,(uname,utype)) 
          result=cursor.fetchall()
          for row in result:
              result1=row[0]
    except Exception as e:
       print(e)

    if result1==pwd:
       if utype=="LDC":
          return render_template('LDChome.html',user=uname)
       elif utype=="Retailer":
          return render_template('Retailerhome.html',user=uname)
       elif utype=="Miner":
          return render_template('Minerhome.html',user=uname)
       elif utype=="Producer":
          return render_template('Retailerhome.html',user=uname)
       else:
          return render_template('Adminhome.html',user=uname)
         
    else:
       flash("Invalid login credentials")
       return render_template('login.html')
  
@app.route('/adminhome')
def adminhome():
    return render_template('AdminHome.html',user=uname)


@app.route('/backadminhome', methods=['GET', 'POST'])
def backadminhome():
    global uname
    return render_template('login.html')


@app.route('/backldchome', methods=['GET', 'POST'])
def backldchome():
    global uname
    return render_template('login.html')

@app.route('/backminerhome', methods=['GET', 'POST'])
def backminerhome():
    global uname
    return render_template('login.html')

@app.route('/backretailerhome', methods=['GET', 'POST'])
def backretailerhome():
    global uname
    return render_template('login.html')

@app.route('/addproducer1')
def addproducer1():
    return render_template('addproducer.html',
                           title='ADD PRODUCER')

@app.route('/backproducer', methods=['GET', 'POST'])
def backproducer():
    global uname
    return render_template('Adminhome.html',
                           user=uname)


@app.route('/addproducer', methods=['GET', 'POST'])
def addproducer():
    name = request.form["name"]
    district = request.form["district"]
    username = request.form["username"]
    password1 = request.form["password"]
    password=sha256(password1.encode()).hexdigest()
    cursor=conn.cursor()
    try:
       cursor.execute("INSERT INTO producer(name,district) values(%s,%s);", (name,district)) 
       conn.commit()
       cursor.execute("INSERT INTO login(type,username,password) values(%s,%s,%s);", ("Producer",username,password)) 
       conn.commit()
    except Exception as e:
       conn.rollback()
    post_object = {
        'name': name,
        'district': district,
        'username':username,
        
    }
    tx_data = post_object
    req_fields = ["name", "district","username"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
       
    flash("New Producer added") 
    return render_template('addproducer.html')
                           
   
@app.route('/addretailer1')
def addretailer1():
    return render_template('addretailer.html',
                           title='ADD RETAILER')
 
@app.route('/backretailer', methods=['GET', 'POST'])
def backretailer():
    global uname
    return render_template('Adminhome.html',
                           user=uname)

@app.route('/addretailer', methods=['GET', 'POST'])
def addretailer():
    name = request.form["name"]
    district = request.form["district"]
    username = request.form["username"]
    password1 = request.form["password"]
    password=sha256(password1.encode()).hexdigest()
    
    cursor=conn.cursor()
    try:
       cursor.execute("INSERT INTO retailer(name,district) values(%s,%s);",(name,district)) 
       conn.commit()
       cursor.execute("INSERT INTO login(type,username,password) values(%s,%s,%s);", ("Retailer",username,password)) 
       conn.commit()
    except Exception as e:
       conn.rollback()

    post_object = {
        'name': name,
        'district': district,
        'username': username,
    }
    tx_data = post_object
    req_fields = ["name", "district","username"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
    flash("New Retailer added") 
    return render_template('addretailer.html')

@app.route('/addminer1')
def addminer1():
    return render_template('addminer.html',
                           title='ADD MINER')
 
@app.route('/backminer', methods=['GET', 'POST'])
def backminer():
    global uname
    return render_template('Adminhome.html',
                           user=uname)

@app.route('/addminer', methods=['GET', 'POST'])
def addminer():
    name = request.form["name"]
    
    username = request.form["username"]
    password1 = request.form["password"]
    password=sha256(password1.encode()).hexdigest()
    cursor=conn.cursor()
    try:
       
       cursor.execute("INSERT INTO login(type,username,password) values(%s,%s,%s);", ("Miner",username,password)) 
       conn.commit()
    except Exception as e:
       conn.rollback()

    post_object = {
        'name': name,
        
        'username': username,
    }
    tx_data = post_object
    req_fields = ["name","username"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
    flash("New Miner added") 
    return render_template('addminer.html')



@app.route('/addproduct1')
def addproduct1():
    return render_template('addproduct.html',
                           title='ADD PRODUCT')
@app.route('/backproduct', methods=['GET', 'POST'])
def backproduct():
    global uname
    return render_template('LDChome.html',
                           user=uname)

@app.route('/addproduct', methods=['GET', 'POST'])
def addproduct():
    name = request.form["name"]
    ptype = request.form["ptype"]
   
    cursor=conn.cursor()
    try:
       cursor.execute("INSERT INTO product(name,type) values(%s,%s);",(name,ptype)) 
       conn.commit()
    except Exception as e:
       conn.rollback()
    post_object = {
        'name': name,
        'type': ptype,
    }
    tx_data = post_object
    req_fields = ["name", "type"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
       
       
    flash("New Product added") 
    return render_template('addproduct.html')

@app.route('/addldc1')
def addldc1():
    return render_template('addldc.html',
                           title='ADD LDC')
 
@app.route('/backldc', methods=['GET', 'POST'])
def backldc():
    global uname
    return render_template('Adminhome.html',
                           user=uname)

@app.route('/addldc', methods=['GET', 'POST'])
def addldc():
    supervisor = request.form["supervisor"]
    district = request.form["district"]
    username = request.form["username"]
    password1 = request.form["password"]
    password=sha256(password1.encode()).hexdigest()
    cursor=conn.cursor()
    try:
       cursor.execute("INSERT INTO ldc(supervisor,district) values(%s,%s);",(supervisor,district)) 
       conn.commit()
       cursor.execute("INSERT INTO login(type,username,password) values(%s,%s,%s);", ("LDC",username,password)) 
       conn.commit()
    except Exception as e:
       conn.rollback()
    post_object = {
        'supervisor': supervisor,
        'district': district,
        'username': username,
    }
    tx_data = post_object
    req_fields = ["supervisor", "district","username"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
    flash("New Local Distribution Centre added") 
    return render_template('addldc.html')

@app.route('/addnewbatch1')
def addnewbatch1():
    sql= "select name from product;"
    cursor=conn.cursor()
    try:
       cursor.execute(sql)
       
    except Exception as e:
       conn.rollback()
    return render_template('addnewbatch.html',
                           title='ADD NEW BATCH',product=cursor.fetchall())
 
@app.route('/backaddbatch', methods=['GET', 'POST'])
def backaddbatch():
    global uname
    return render_template('LDChome.html',
                           user=uname)

@app.route('/addnewbatch', methods=['GET', 'POST'])
def addnewbatch():
    name = request.form["name"]
    
    ptype = request.form["ptype"]
    #prodId= request.form["prodId"]
    weight = request.form["weight"]
    producer = request.form["producer"]
    expiry1= datetime.now()+timedelta(minutes=2)
    expiry= expiry1.strftime('%Y-%m-%d %H:%M:%S')
    #expiry = request.form["expiry"]
   
    cursor=conn.cursor()
    try:
       

       sql="SELECT prodId from product where name=%s AND type=%s"
       cursor.execute(sql,(name,ptype))
       result=cursor.fetchall()
       for res in result:
           prodId=res[0]
       cursor.execute("INSERT INTO newbatch(name,type,prodId,weight,producer,expiry) values(%s,%s,%s,%s,%s,%s);",(name,ptype,prodId,weight,producer,expiry)) 
       conn.commit()
       flash("New Batch added")
    except Exception as e:
       print('rrr')
       conn.rollback()
    
    post_object = {
        'name': name,
        'type': ptype,
        'weight':weight,
        'producer':producer,
        'expiry':expiry
    }
    tx_data = post_object
    req_fields = ["name", "type","weight","producer","expiry"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
     
    return render_template('addnewbatch.html')

@app.route('/sellnewbatch1')
def sellnewbatch1():
    return render_template('sellnewbatch.html',
                           title='SELL BATCH')
 
@app.route('/backsellbatch', methods=['GET', 'POST'])
def backsellbatch():
    global uname
    return render_template('LDChome.html',
                           user=uname)

@app.route('/sellnewbatch', methods=['GET', 'POST'])
def sellnewbatch():
    prodid=request.form["prodid"]
    weight1 = request.form["weight"]
    retailer= request.form["retailer"]
    cursor=conn.cursor()
    try:
       flag1=0
       sql2="SELECT SUM(weight) as pquantity2 FROM newbatch where prodId=%s;"
       cursor.execute(sql2,(prodid)) 
       pquantity1=cursor.fetchall()
       for row in pquantity1:
           pquantity=row[0]
       print(pquantity)
       weight=int(weight1)
       date=datetime.now()

       # if product asked for is not in newbatch 
      
       s="SELECT count(prodId) as countofprod FROM newbatch;"
       cursor.execute(s) 
       countofprod=cursor.fetchall()
       for row in countofprod:
           countofprod1=row[0]
       print("count of products",countofprod1)

       found=0
       
       cursor.execute("SELECT prodId from newbatch")
       pres1=cursor.fetchall()
       for row in pres1:
           pres=row[0]
           #print("pres",pres)
           if pres==prodid:
              found=1
       if found==0:
          flash("Product not available")       

       if pquantity>=weight:
          cursor.execute("INSERT INTO sellnewbatch(prodid,weight,retailer,date) values(%s,%s,%s,%s);",(prodid,weight1,retailer,date)) 
          conn.commit()

          while flag1==0:   
                cursor.execute("SELECT weight from newbatch where prodId=%s",(prodid))
                pres1=cursor.fetchone()
                pres=pres1[0]
                pres=int(pres)
                
                cursor.execute("SELECT batchno from newbatch where prodId=%s",(prodid))
                batch1=cursor.fetchone()
                batch=batch1[0]
                if pres>=weight:
                   temp1=pres-weight
                   temp=str(temp1)
                   cursor.execute("UPDATE newbatch SET weight=%s where batchno=%s;",(temp,batch))
                   flag1=1
                else: 
                   weight=weight-pres
                   cursor.execute("DELETE FROM newbatch where batchno=%s;",(batch)) 
          flash("Batch sold out") 
       else:
           flash("Sorry, req quantity not available !") 
           
    except Exception as e:
       print(e)
       conn.rollback()

    post_object = {
        'prodid':prodid,
        'weight':weight,
        'retailer':retailer,
    }
    tx_data = post_object
    req_fields = ["weight","retailer","prodid"]

    for field in req_fields:
        if not tx_data.get(field):
            return "Invalid transaction data", 404

    tx_data["timestamp"] = time.time()
    blockchain.add_new_trans(tx_data)
    
    
    return render_template('sellnewbatch.html')


@app.route('/viewproduct1', methods=['GET','POST'])
def viewproduct1():
    sql= "SELECT prodId from product;"
    cursor = conn.cursor()
    try:
       cursor.execute(sql)
    except Exception as e:
       print(e)
      
    return render_template("viewproducts.html",ids=cursor.fetchall())

@app.route('/backviewproduct', methods=['GET', 'POST'])
def backviewproduct():
    global uname
    return render_template('LDChome.html',
                           user=uname)

@app.route('/viewproduct', methods=['GET','POST'])
def viewproduct():
    prodid=request.form["ids"]
    cursor = conn.cursor()
    try:
          sql="SELECT name FROM product where prodId=%s;"
          cursor.execute(sql,(prodid)) 
          pname1=cursor.fetchall()
          for row in pname1:
              pname=row[0]
          sql1="SELECT type FROM product where prodId=%s;"
          cursor.execute(sql1,(prodid)) 
          ptype1=cursor.fetchall()
          for row in ptype1:
              ptype=row[0]

          sql2="SELECT SUM(weight) as pquantity2 FROM newbatch where prodId=%s;"
          cursor.execute(sql2,(prodid)) 
          pquantity1=cursor.fetchall()
          for row in pquantity1:
              pquantity=row[0]
          print(pquantity)
    except Exception as e:
       print(e)   
    return render_template("productdetails.html",pname=pname,ptype=ptype,pquantity=pquantity)
app.route('/display', methods=['GET','POST'])
def display():
    fh1= open("details.txt", "r")
    fh2=json.dumps(fh1.readlines())
    
    return fh2

@app.route('/chain', methods=['GET'])
def get_chain():
    chain_data = []
    for block in blockchain.chain:
        chain_data.append(block.__dict__)
    return json.dumps({"length": len(chain_data),
                       "chain": chain_data,
                       "peers": list(peers)})


@app.route('/mine', methods=['GET'])
def mine_unconf_trans():
    result = blockchain.mine()
    if not result:
        return "No transactions to mine"
    else:
        
        chain_length = len(blockchain.chain)
        consensus()
        if chain_length == len(blockchain.chain):
        
            announce_new_block(blockchain.final_block)
        return "Block #{} is mined.".format(blockchain.final_block.index)


@app.route('/index')
def index():
	return render_template('index.html',
                           title='CERTIFICATES')
         
def create_chain(chain_dump):
    generated_blockchain = Blockchain()
    generated_blockchain.build_genesis_block()
    for idx, block_data in enumerate(chain_dump):
        if idx == 0:
            continue  
        block = Block(block_data["index"],
                      block_data["trans"],
                      block_data["timestamp"],
                      block_data["prev_hash"],
                      block_data["nonce"])
        proof = block_data['hash']
        added = generated_blockchain.add_new_block(block, proof)
        if not added:
            raise Exception("The chain dump is tampered!!")
    return generated_blockchain



@app.route('/add_block', methods=['POST'])
def verify_block():
    block_data = request.get_json()
    block = Block(block_data["index"],
                  block_data["trans"],
                  block_data["timestamp"],
                  block_data["prev_hash"],
                  block_data["nonce"])
    proof = block_data['hash']
    added = blockchain.add_new_block(block, proof)
    if not added:
        return "The block was rejected", 400
    return "Block included to chain", 201

    new_tx_address = "{}/new_transaction".format(CONNECTED_NODE_ADDRESS)
    requests.post(new_tx_address,
                  json=post_object,
                  headers={'Content-type': 'application/json'})
    return redirect('/index')


def consensus():    
   global blockchain
    longest_chain = None
    current_len = len(blockchain.chain) 
    for node in peers:
        response = requests.get('{}chain'.format(node))
        length = response.json()['length']
        chain = response.json()['chain']
        if length > current_len and blockchain.check_chain_validity(chain):
            current_len = length
            longest_chain = chain
    if longest_chain:
        blockchain = longest_chain
        return True
   return False

def announce_new_block(block):
    for peer in peers:
        url = "{}add_block".format(peer)
        headers = {'Content-Type': "application/json"}
        requests.post(url,
                      data=json.dumps(block.__dict__, sort_keys=True),
                      headers=headers)
def sensor():
    """ Function for test purposes. """
    #print("Scheduler is alive!")
    cursor=conn.cursor()
    try:
       sql= "SELECT expiry from newbatch;"
       sql1="DELETE FROM newbatch where expiry = %s;" 
       sql2="SELECT batchno from newbatch where expiry = %s;"
       cursor.execute(sql)  
       result=cursor.fetchall()
       for res in result:
           res1=res[0]         
           if res1 < datetime.now():
              print("Expired")
              cursor.execute(sql2,(res1))
              result2=cursor.fetchone()   
              resu=result2[0];   
              cursor.execute(sql1,(res1))
              conn.commit() 
              post_object = {
                  'Batch_no': resu,
                  'Status' : "Expired",
                  }
              tx_data = post_object              
              tx_data["timestamp"] = time.time()
              blockchain.add_new_trans(tx_data)
    except Exception as e:
       print(e)
       conn.rollback()
sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=1)
sched.start()
