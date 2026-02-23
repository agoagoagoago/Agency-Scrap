from flask import Flask, render_template, jsonify
import db

app = Flask(__name__)


@app.route("/")
def dashboard():
    db.init_db()
    latest = db.get_latest_run()
    history = db.get_run_history(30)

    if latest:
        metrics = {
            "run_at": latest[0],
            "total_agencies": latest[1],
            "total_agents": latest[2],
            "new_agencies": latest[3],
            "removed_agencies": latest[4],
            "new_agents": latest[5],
            "removed_agents": latest[6],
            "new_agency_names": latest[7] or [],
            "removed_agency_names": latest[8] or [],
            "status": latest[9],
        }
    else:
        metrics = None

    runs = []
    for r in history:
        runs.append({
            "run_at": r[0],
            "total_agencies": r[1],
            "total_agents": r[2],
            "new_agencies": r[3],
            "removed_agencies": r[4],
            "new_agents": r[5],
            "removed_agents": r[6],
            "status": r[7],
        })

    return render_template("dashboard.html", metrics=metrics, runs=runs)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})
