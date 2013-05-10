from flask import  render_template, request, flash, abort, redirect, url_for, jsonify, session, render_template_string
from helix_monitor import app, helixEVListener

@app.route('/')
def index():
    return redirect(url_for("home"))

@app.route('/home')
def home():
    resource = helixEVListener.getResources()
#    ev =  helixEVListener.getEV(resource[0])
    ev =  helixEVListener.getEVAll()
    return render_template('index.html', ev = ev )

#def render_ev(ev):
#    return render_template('index.html', ev = ev )