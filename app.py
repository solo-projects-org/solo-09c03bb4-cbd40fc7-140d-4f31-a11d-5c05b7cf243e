#!/usr/bin/env python3
"""灌装线欠重累计复检系统。

上传「抽检序号,整数克重」CSV 存入 SQLite：
- 同号可更正；新号只允许往后追加（必须大于当前最大序号）。
- 按序号顺序把 (499 - 实测克重) 计入累计，累计 < 0 时归零；
  累计 >= 20 克时记录一条复检事件（本轮累计转正起点序号 -> 触发序号、累计克数），随后清零。
- 更正触发增量复算：跳过首处改动前的称重（直接取 run_state 保存的状态）；
  复算途中若已越过全部更正、当前序号仍是旧数据，且 (累计值, 本轮起点) 与旧结果一致，
  则之后的旧结果直接沿用，不再重读重量；若更后面还有新增序号，则从旧末尾状态继续处理。
- 每次上传一个事务，重量与复检清单一同生效；全局锁 + BEGIN IMMEDIATE 保证
  并发上传按生效顺序串行处理；提交即持久化，失败或重启后仍可查到最近一次成功结果。
"""

import csv
import io
import os
import sqlite3
import threading

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

NOMINAL_GRAMS = 499   # 标称克重
TRIGGER_GRAMS = 20    # 累计触发阈值（克）

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    applied_at    TEXT NOT NULL DEFAULT (datetime('now')),
    rows          INTEGER NOT NULL,
    additions     INTEGER NOT NULL,
    corrections   INTEGER NOT NULL,
    first_changed INTEGER,
    resumed_at    INTEGER
);
CREATE TABLE IF NOT EXISTS weighings (
    seq    INTEGER PRIMARY KEY,
    weight INTEGER NOT NULL CHECK (weight >= 0)
);
CREATE TABLE IF NOT EXISTS run_state (
    seq         INTEGER PRIMARY KEY,
    cumulative  INTEGER NOT NULL,
    round_start INTEGER
);
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id  INTEGER NOT NULL REFERENCES batches(id),
    start_seq INTEGER NOT NULL,
    end_seq   INTEGER NOT NULL,
    grams     INTEGER NOT NULL
);
"""

# 同一进程内串行化上传；跨连接再由 BEGIN IMMEDIATE 的写锁兜底。
_upload_lock = threading.Lock()


# ---------------------------------------------------------------- CSV 解析

def parse_csv(text):
    """解析「序号,克重」CSV，允许首行表头。返回 [(seq, weight), ...]，非法即抛 ValueError。"""
    rows = []
    seen = set()
    reader = csv.reader(io.StringIO(text))
    for lineno, raw in enumerate(reader, start=1):
        if not raw or all(not cell.strip() for cell in raw):
            continue
        if len(raw) != 2:
            raise ValueError(f"第 {lineno} 行需要恰好两列（序号,克重），实际 {len(raw)} 列")
        a, b = raw[0].strip(), raw[1].strip()
        seq = weight = None
        try:
            seq = int(a)
        except ValueError:
            pass
        try:
            weight = int(b)
        except ValueError:
            pass
        if seq is None or weight is None:
            if lineno == 1 and seq is None and weight is None:
                continue  # 表头行
            raise ValueError(f"第 {lineno} 行不是整数：{a!r}, {b!r}")
        if seq < 1:
            raise ValueError(f"第 {lineno} 行序号必须为正整数：{seq}")
        if weight < 0:
            raise ValueError(f"第 {lineno} 行克重不能为负：{weight}")
        if seq in seen:
            raise ValueError(f"序号 {seq} 在文件内重复（第 {lineno} 行）")
        seen.add(seq)
        rows.append((seq, weight))
    if not rows:
        raise ValueError("CSV 中没有数据行")
    return rows


# ---------------------------------------------------------------- 核心计算

def _accumulate(states, events, cum, rs, seq, weight):
    """处理一条称重：累加差值、归零、触发复检事件。返回新的 (cum, rs)。"""
    cum += NOMINAL_GRAMS - weight
    if cum < 0:
        cum = 0
    if cum == 0:
        rs = None
    else:
        if rs is None:
            rs = seq          # 本轮累计转正的位置
        if cum >= TRIGGER_GRAMS:
            events.append((rs, seq, cum))
            cum, rs = 0, None
    states.append((seq, cum, rs))
    return cum, rs


def _insert_results(conn, batch_id, states, events):
    conn.executemany(
        "INSERT INTO run_state (seq, cumulative, round_start) VALUES (?,?,?)", states)
    conn.executemany(
        "INSERT INTO events (batch_id, start_seq, end_seq, grams) VALUES (?,?,?,?)",
        [(batch_id, s, e, g) for s, e, g in events])


def _recompute(conn, changed, batch_id, old_max):
    """从首个改动序号起增量复算。changed 为实际变化的序号（新增或克重改变），已排序。

    - 跳过首处改动前的称重：直接取 run_state 中保存的改动前状态。
    - 复算途中若已越过全部更正、当前序号仍属旧数据，且 (累计值, 本轮起点)
      与旧结果一致，则之后的旧结果直接沿用，不再重读重量。
    - 若旧末尾之后还有本次新增的序号，则从旧末尾状态继续处理新增部分。
    返回 (first_changed, resumed_at)。
    """
    first = changed[0]
    corrections = [s for s in changed if old_max is not None and s <= old_max]
    corrections_max = max(corrections) if corrections else None

    prev = conn.execute(
        "SELECT cumulative, round_start FROM run_state WHERE seq < ? "
        "ORDER BY seq DESC LIMIT 1", (first,)).fetchone()
    cum = prev[0] if prev else 0
    rs = prev[1] if prev else None

    states, events = [], []
    resumed_at = None
    # 游标逐行取数，break 后剩余旧称重不会被读取。
    cur = conn.execute(
        "SELECT seq, weight FROM weighings WHERE seq >= ? ORDER BY seq", (first,))
    for seq, weight in cur:
        if (corrections_max is not None
                and seq > corrections_max
                and seq <= old_max):
            old = conn.execute(
                "SELECT cumulative, round_start FROM run_state WHERE seq < ? "
                "ORDER BY seq DESC LIMIT 1", (seq,)).fetchone()
            if old is not None and old[0] == cum and old[1] == rs:
                resumed_at = seq   # 与旧结果汇合：旧尾段直接沿用
                break
        cum, rs = _accumulate(states, events, cum, rs, seq, weight)

    if resumed_at is None:
        conn.execute("DELETE FROM run_state WHERE seq >= ?", (first,))
        conn.execute("DELETE FROM events WHERE end_seq >= ?", (first,))
        _insert_results(conn, batch_id, states, events)
    else:
        conn.execute(
            "DELETE FROM run_state WHERE seq >= ? AND seq < ?", (first, resumed_at))
        conn.execute(
            "DELETE FROM events WHERE end_seq >= ? AND end_seq < ?", (first, resumed_at))
        _insert_results(conn, batch_id, states, events)
        # 旧尾段 [resumed_at, old_max] 原样保留；其后的新增序号从旧末尾状态继续。
        tail = conn.execute(
            "SELECT seq, weight FROM weighings WHERE seq > ? ORDER BY seq",
            (old_max,)).fetchall()
        if tail:
            end_state = conn.execute(
                "SELECT cumulative, round_start FROM run_state WHERE seq = ?",
                (old_max,)).fetchone()
            cum, rs = end_state[0], end_state[1]
            states2, events2 = [], []
            for seq, weight in tail:
                cum, rs = _accumulate(states2, events2, cum, rs, seq, weight)
            _insert_results(conn, batch_id, states2, events2)
    return first, resumed_at


def apply_upload(conn, rows):
    """在一个事务内应用一批称重并增量复算。调用方负责加锁与事务。"""
    old_max = conn.execute("SELECT MAX(seq) FROM weighings").fetchone()[0]
    changed = []
    additions = corrections = 0
    for seq, weight in rows:
        old = conn.execute(
            "SELECT weight FROM weighings WHERE seq = ?", (seq,)).fetchone()
        if old is None:
            if old_max is not None and seq <= old_max:
                raise ValueError(
                    f"新序号 {seq} 必须大于当前最大序号 {old_max}（新号只往后追加）")
            additions += 1
            changed.append(seq)
        elif old[0] != weight:
            corrections += 1
            changed.append(seq)
        # 同号同重：视为无改动
    cur = conn.execute(
        "INSERT INTO batches (rows, additions, corrections) VALUES (?,?,?)",
        (len(rows), additions, corrections))
    batch_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO weighings (seq, weight) VALUES (?,?) "
        "ON CONFLICT(seq) DO UPDATE SET weight = excluded.weight", rows)
    first_changed = resumed_at = None
    if changed:
        first_changed, resumed_at = _recompute(conn, sorted(changed), batch_id, old_max)
        conn.execute(
            "UPDATE batches SET first_changed = ?, resumed_at = ? WHERE id = ?",
            (first_changed, resumed_at, batch_id))
    return {"batch_id": batch_id, "additions": additions, "corrections": corrections,
            "first_changed": first_changed, "resumed_at": resumed_at}


# ---------------------------------------------------------------- Flask 应用

def create_app(db_path=None):
    app = Flask(__name__)
    app.config["DB_PATH"] = (
        db_path or os.environ.get("FILLING_DB")
        or os.path.join(os.path.dirname(os.path.abspath(__file__)), "filling.db"))
    app.secret_key = os.environ.get("SECRET_KEY", "filling-line-dev-key")

    db_dir = os.path.dirname(app.config["DB_PATH"])
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with sqlite3.connect(app.config["DB_PATH"]) as conn:
        conn.executescript(SCHEMA)

    def connect():
        conn = sqlite3.connect(app.config["DB_PATH"], timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def process_text(text):
        """解析并生效一批 CSV；成功返回结果 dict，数据问题抛 ValueError。"""
        rows = parse_csv(text)
        with _upload_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = apply_upload(conn, rows)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------ 页面

    @app.get("/")
    def index():
        conn = connect()
        try:
            state = conn.execute(
                "SELECT seq, cumulative, round_start FROM run_state "
                "ORDER BY seq DESC LIMIT 1").fetchone()
            weighing_count = conn.execute(
                "SELECT COUNT(*) FROM weighings").fetchone()[0]
            event_count = conn.execute(
                "SELECT COUNT(*) FROM events").fetchone()[0]
            events = conn.execute(
                "SELECT e.id, e.start_seq, e.end_seq, e.grams, e.batch_id, b.applied_at "
                "FROM events e JOIN batches b ON b.id = e.batch_id "
                "ORDER BY e.end_seq DESC LIMIT 200").fetchall()
            recent = conn.execute(
                "SELECT seq, weight FROM weighings ORDER BY seq DESC LIMIT 50").fetchall()
            batches = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT 20").fetchall()
        finally:
            conn.close()
        return render_template(
            "index.html", state=state, weighing_count=weighing_count,
            event_count=event_count, events=events, recent=recent, batches=batches,
            nominal=NOMINAL_GRAMS, trigger=TRIGGER_GRAMS)

    @app.post("/upload")
    def upload():
        f = request.files.get("file")
        try:
            if f is not None and f.filename:
                text = f.read().decode("utf-8-sig")
            else:
                text = request.form.get("csv_text", "")
            result = process_text(text)
        except ValueError as exc:
            flash(f"上传未生效：{exc}", "error")
            return redirect(url_for("index"))
        except Exception:
            app.logger.exception("上传处理失败")
            flash("内部错误，本次上传未生效", "error")
            return redirect(url_for("index"))
        flash("批次 #{} 已生效：新增 {} 条、更正 {} 条{}".format(
            result["batch_id"], result["additions"], result["corrections"],
            _recompute_note(result)), "ok")
        return redirect(url_for("index"))

    # ------------------------------------------------------------ API

    @app.post("/api/upload")
    def api_upload():
        text = request.get_data(as_text=True)
        try:
            result = process_text(text)
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("上传处理失败")
            return jsonify(ok=False, error="内部错误，本次上传未生效"), 500
        return jsonify(ok=True, **result)

    @app.get("/api/state")
    def api_state():
        conn = connect()
        try:
            state = conn.execute(
                "SELECT seq, cumulative, round_start FROM run_state "
                "ORDER BY seq DESC LIMIT 1").fetchone()
            weighing_count = conn.execute(
                "SELECT COUNT(*) FROM weighings").fetchone()[0]
            event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        finally:
            conn.close()
        return jsonify(
            last_seq=state["seq"] if state else None,
            cumulative=state["cumulative"] if state else 0,
            round_start=state["round_start"] if state else None,
            weighing_count=weighing_count, event_count=event_count)

    @app.get("/api/events")
    def api_events():
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT id, batch_id, start_seq, end_seq, grams FROM events "
                "ORDER BY end_seq").fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/weighings")
    def api_weighings():
        limit = min(int(request.args.get("limit", 200)), 10000)
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT seq, weight FROM weighings ORDER BY seq LIMIT ?", (limit,)
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.get("/healthz")
    def healthz():
        return "ok"

    return app


def _recompute_note(result):
    if result["first_changed"] is None:
        return "；无实际改动，未触发复算"
    note = f"；复算自序号 {result['first_changed']} 起"
    if result["resumed_at"] is not None:
        note += f"，自 {result['resumed_at']} 起沿用旧结果"
    else:
        note += "，已复算至末尾"
    return note


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), threaded=True)
