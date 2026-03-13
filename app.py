from flask import Flask, render_template, jsonify, request
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


@app.route("/scorecards")
def scorecards():
    db.init_db()
    days = request.args.get("days", 30, type=int)
    if days not in (30, 60, 90):
        days = 30
    show_all = request.args.get("all", 0, type=int) == 1

    all_scorecards = db.get_agency_scorecards(days)

    if show_all:
        gainers = [s for s in all_scorecards if s["net_change"] > 0]
        losers = [s for s in all_scorecards if s["net_change"] < 0]
        truncated = False
    else:
        gainers = [s for s in all_scorecards if s["net_change"] > 0][:25]
        losers = [s for s in all_scorecards if s["net_change"] < 0][-25:]
        total_gainers = sum(1 for s in all_scorecards if s["net_change"] > 0)
        total_losers = sum(1 for s in all_scorecards if s["net_change"] < 0)
        truncated = total_gainers > 25 or total_losers > 25

    return render_template("scorecards.html", gainers=gainers, losers=losers,
                           days=days, show_all=show_all, truncated=truncated)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})
