#!/usr/bin/env python3
"""灌装线欠重累计复检系统。

支持两类称重 CSV（首行表头均可省，克重一律整数）：
- 两列「抽检序号,克重」：旧秤/旧库口径，克重按**直接净重**处理（source='direct'）；
- 三列「抽检序号,毛重,包装批号」：新秤只导出毛重，按包装批登记的皮重扣除，
  净重 = 毛重 − 皮重（source='gross'，记录毛重、包装批号与所用皮重）。

包装批皮重在网页登记/更正（「包装批号,皮重」两列，可一次多行）：
- 皮重更正时只重新换算**仍引用该包装批**的毛重称重，直接净重称重不受影响；
- 缺皮重（毛重 CSV 引用了未登记的包装批）或算出负净重，整次操作退回，原状态不变。

通用规则：
- 同号可更正、新号只允许往后追加（必须大于当前最大序号），文件内序号不得重复；
- 同号以最后一次成功上传的来源为准：即使净重相同，direct/gross 互切也属更正，
  必须换掉来源行；
- 按序号顺序把 (499 − 净重) 计入累计，累计 < 0 归零；累计 >= 20 克记录一条复检
  事件（本轮累计转正起点序号 -> 触发序号、累计克数），随后清零；
- 更正触发增量复算：跳过首处改动前的称重（直接取 run_state 保存的状态）；
  复算途中若已越过全部更正、当前序号仍是旧数据，且 (累计值, 本轮起点) 与旧结果
  一致，则之后的旧结果直接沿用；若更后面还有新增序号，则从旧末尾状态继续处理；
- 每次上传一个事务：皮重、称重来源、净重与复检清单一同提交，失败整体回滚；
  进程内全局锁 + BEGIN IMMEDIATE 保证并发操作按生效顺序串行处理。
旧库（weighings 仅有 weight 列）启动时自动迁移为直接净重口径。
"""

import csv
import io
import os
import sqlite3
import threading

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

NOMINAL_GRAMS = 499   # 标称克重
TRIGGER_GRAMS = 20    # 累计触发阈值（克）

SOURCE_DIRECT = "direct"   # 两列 CSV / 旧库：克重即净重
SOURCE_GROSS = "gross"     # 三列 CSV：毛重扣皮重后的净重

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    applied_at    TEXT NOT NULL DEFAULT (datetime('now')),
    op_type       TEXT NOT NULL DEFAULT 'net'
                      CHECK (op_type IN ('net', 'gross', 'tare')),
    rows          INTEGER NOT NULL,
    additions     INTEGER NOT NULL,
    corrections   INTEGER NOT NULL,
    first_changed INTEGER,
    resumed_at    INTEGER
);
CREATE TABLE IF NOT EXISTS packaging_batches (
    batch_no   TEXT PRIMARY KEY,
    tare       INTEGER NOT NULL CHECK (tare >= 0),
    applied_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS weighings (
    seq          INTEGER PRIMARY KEY,
    net_weight   INTEGER NOT NULL CHECK (net_weight >= 0),
    gross_weight INTEGER CHECK (gross_weight IS NULL OR gross_weight >= 0),
    batch_no     TEXT REFERENCES packaging_batches(batch_no),
    source       TEXT NOT NULL CHECK (source IN ('direct', 'gross'))
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

def _parse_grid(text, ncols, num_idx, key_idx, label):
    """通用 CSV 解析：要求恰好 ncols 列，num_idx 列出的列必须是整数。

    首行若数值列无法解析为整数则按表头跳过；key_idx 列用于文件内重复检查。
    返回原始行（数值列已转 int，文本列已 strip），非法即抛 ValueError。
    """
    rows, seen = [], set()
    reader = csv.reader(io.StringIO(text))
    for lineno, raw in enumerate(reader, start=1):
        if not raw or all(not cell.strip() for cell in raw):
            continue
        if len(raw) != ncols:
            raise ValueError(
                f"第 {lineno} 行需要恰好 {ncols} 列，实际 {len(raw)} 列")
        cells = [c.strip() for c in raw]
        nums = {}
        header = False
        for i in num_idx:
            try:
                nums[i] = int(cells[i])
            except ValueError:
                header = True
        if header:
            if lineno == 1:
                continue  # 表头行
            raise ValueError(f"第 {lineno} 行的整数列无法解析：{cells!r}")
        key = cells[key_idx]
        if key in seen:
            raise ValueError(f"{label} {key} 在文件内重复（第 {lineno} 行）")
        seen.add(key)
        rows.append((lineno, cells, nums))
    if not rows:
        raise ValueError("CSV 中没有数据行")
    return rows


def parse_net_csv(text):
    """解析两列「序号,净重」CSV，返回 [(seq, net_weight), ...]。"""
    out = []
    for lineno, cells, nums in _parse_grid(text, 2, (0, 1), 0, "序号"):
        seq, net = nums[0], nums[1]
        if seq < 1:
            raise ValueError(f"第 {lineno} 行序号必须为正整数：{seq}")
        if net < 0:
            raise ValueError(f"第 {lineno} 行克重不能为负：{net}")
        out.append((seq, net))
    return out


def parse_gross_csv(text):
    """解析三列「序号,毛重,包装批号」CSV，返回 [(seq, gross, batch_no), ...]。"""
    out = []
    for lineno, cells, nums in _parse_grid(text, 3, (0, 1), 0, "序号"):
        seq, gross = nums[0], nums[1]
        batch_no = cells[2]
        if seq < 1:
            raise ValueError(f"第 {lineno} 行序号必须为正整数：{seq}")
        if gross < 0:
            raise ValueError(f"第 {lineno} 行毛重不能为负：{gross}")
        if not batch_no:
            raise ValueError(f"第 {lineno} 行缺少包装批号")
        out.append((seq, gross, batch_no))
    return out


def parse_tare_csv(text):
    """解析两列「包装批号,皮重」CSV，返回 [(batch_no, tare), ...]。"""
    out = []
    for lineno, cells, nums in _parse_grid(text, 2, (1,), 0, "包装批号"):
        batch_no, tare = cells[0], nums[1]
        if not batch_no:
            raise ValueError(f"第 {lineno} 行缺少包装批号")
        if tare < 0:
            raise ValueError(f"第 {lineno} 行皮重不能为负：{tare}")
        out.append((batch_no, tare))
    return out


def sniff_columns(text):
    """取首个非空行的列数：称重 CSV 只允许 2 列或 3 列。"""
    for raw in csv.reader(io.StringIO(text)):
        if raw and any(cell.strip() for cell in raw):
            return len(raw)
    raise ValueError("CSV 中没有数据行")


# ---------------------------------------------------------------- 旧库迁移

def _migrate_legacy(conn):
    """旧库 weighings(seq, weight) 迁移为净重口径；旧 batches 补 op_type。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(weighings)")}
    if cols and "net_weight" not in cols:
        conn.execute("ALTER TABLE weighings RENAME TO weighings_old")
        conn.execute("""
            CREATE TABLE weighings (
                seq          INTEGER PRIMARY KEY,
                net_weight   INTEGER NOT NULL CHECK (net_weight >= 0),
                gross_weight INTEGER CHECK (gross_weight IS NULL OR gross_weight >= 0),
                batch_no     TEXT REFERENCES packaging_batches(batch_no),
                source       TEXT NOT NULL CHECK (source IN ('direct', 'gross'))
            )""")
        conn.execute("""
            INSERT INTO weighings (seq, net_weight, gross_weight, batch_no, source)
            SELECT seq, weight, NULL, NULL, 'direct' FROM weighings_old""")
        conn.execute("DROP TABLE weighings_old")
    bcols = {r[1] for r in conn.execute("PRAGMA table_info(batches)")}
    if bcols and "op_type" not in bcols:
        conn.execute(
            "ALTER TABLE batches ADD COLUMN op_type TEXT NOT NULL DEFAULT 'net'")


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
    """从首个改动序号起增量复算。changed 为实际变化的序号（新增或净重/来源改变），已排序。

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
        "SELECT seq, net_weight FROM weighings WHERE seq >= ? ORDER BY seq", (first,))
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
            "SELECT seq, net_weight FROM weighings WHERE seq > ? ORDER BY seq",
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


def apply_weighings(conn, items, source):
    """在一个事务内应用一批称重并增量复算。调用方负责加锁与事务。

    source='direct' 时 items 为 [(seq, net_weight), ...]；
    source='gross' 时 items 为 [(seq, gross, batch_no), ...]，当场扣皮重算净重。
    """
    old_max = conn.execute("SELECT MAX(seq) FROM weighings").fetchone()[0]
    records, changed = [], []
    additions = corrections = 0
    for item in items:
        if source == SOURCE_GROSS:
            seq, gross, batch_no = item
            pb = conn.execute(
                "SELECT tare FROM packaging_batches WHERE batch_no = ?",
                (batch_no,)).fetchone()
            if pb is None:
                raise ValueError(
                    f"包装批 {batch_no} 尚未登记皮重，序号 {seq} 的毛重无法换算"
                    "（缺皮重，整次操作退回）")
            net = gross - pb[0]
            if net < 0:
                raise ValueError(
                    f"序号 {seq} 净重为负（毛重 {gross} − 皮重 {pb[0]} = {net}），"
                    "整次操作退回")
            record = (seq, net, gross, batch_no, SOURCE_GROSS)
        else:
            seq, net = item
            record = (seq, net, None, None, SOURCE_DIRECT)
        old = conn.execute(
            "SELECT net_weight, gross_weight, batch_no, source "
            "FROM weighings WHERE seq = ?", (seq,)).fetchone()
        if old is None:
            if old_max is not None and seq <= old_max:
                raise ValueError(
                    f"新序号 {seq} 必须大于当前最大序号 {old_max}（新号只往后追加）")
            additions += 1
            changed.append(seq)
        elif tuple(old) != tuple(record[1:]):
            # 同号以最后一次成功上传的来源为准：净重相同但来源/毛重/批号不同，照样更正。
            corrections += 1
            changed.append(seq)
            # 同号且净重、来源、毛重、批号完全一致：视为无改动
        records.append(record)
    cur = conn.execute(
        "INSERT INTO batches (op_type, rows, additions, corrections) "
        "VALUES (?,?,?,?)",
        ("gross" if source == SOURCE_GROSS else "net",
         len(items), additions, corrections))
    batch_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO weighings (seq, net_weight, gross_weight, batch_no, source) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(seq) DO UPDATE SET net_weight = excluded.net_weight, "
        "gross_weight = excluded.gross_weight, batch_no = excluded.batch_no, "
        "source = excluded.source", records)
    first_changed = resumed_at = None
    if changed:
        first_changed, resumed_at = _recompute(conn, sorted(changed), batch_id, old_max)
        conn.execute(
            "UPDATE batches SET first_changed = ?, resumed_at = ? WHERE id = ?",
            (first_changed, resumed_at, batch_id))
    return {"batch_id": batch_id, "op_type": source,
            "additions": additions, "corrections": corrections,
            "first_changed": first_changed, "resumed_at": resumed_at}


def apply_tares(conn, items):
    """在一个事务内登记/更正包装批皮重，并重算受影响的毛重称重。

    items 为 [(batch_no, tare), ...]。皮重更正只重新换算仍引用该包装批的称重；
    任一引用称重算出负净重即抛 ValueError，整次操作（含已写入的皮重）回滚。
    """
    new_batches = changed_batches = 0
    tare_updates, affected = [], set()
    for batch_no, tare in items:
        old = conn.execute(
            "SELECT tare FROM packaging_batches WHERE batch_no = ?",
            (batch_no,)).fetchone()
        if old is None:
            new_batches += 1
            tare_updates.append((batch_no, tare))
        elif old[0] != tare:
            changed_batches += 1
            tare_updates.append((batch_no, tare))
        # 皮重未变：无需换算
    cur = conn.execute(
        "INSERT INTO batches (op_type, rows, additions, corrections) "
        "VALUES ('tare', ?, ?, ?)",
        (len(items), new_batches, changed_batches))
    batch_id = cur.lastrowid

    # 先做换算校验：负净重直接失败，此时尚未写入任何皮重。
    for batch_no, tare in tare_updates:
        refs = conn.execute(
            "SELECT seq, gross_weight FROM weighings "
            "WHERE batch_no = ? AND source = 'gross' ORDER BY seq",
            (batch_no,)).fetchall()
        for seq, gross in refs:
            net = gross - tare
            if net < 0:
                raise ValueError(
                    f"包装批 {batch_no} 皮重更正为 {tare} 后，序号 {seq} 净重为负"
                    f"（毛重 {gross} − 皮重 {tare} = {net}），整次操作退回")
            affected.add(seq)

    conn.executemany(
        "INSERT INTO packaging_batches (batch_no, tare) VALUES (?,?) "
        "ON CONFLICT(batch_no) DO UPDATE SET tare = excluded.tare, "
        "updated_at = datetime('now')", tare_updates)
    if affected:
        # 只重新换算仍引用这些包装批的称重；直接净重与其他包装批的称重不动。
        conn.executemany(
            "UPDATE weighings SET net_weight = gross_weight - "
            "(SELECT tare FROM packaging_batches pb WHERE pb.batch_no = weighings.batch_no) "
            "WHERE seq = ?", [(seq,) for seq in affected])

    old_max = conn.execute("SELECT MAX(seq) FROM weighings").fetchone()[0]
    first_changed = resumed_at = None
    if affected:
        changed = sorted(affected)
        first_changed, resumed_at = _recompute(conn, changed, batch_id, old_max)
        conn.execute(
            "UPDATE batches SET first_changed = ?, resumed_at = ? WHERE id = ?",
            (first_changed, resumed_at, batch_id))
    return {"batch_id": batch_id, "op_type": "tare",
            "additions": new_batches, "corrections": changed_batches,
            "first_changed": first_changed, "resumed_at": resumed_at,
            "affected_seqs": sorted(affected)}


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
        _migrate_legacy(conn)
        conn.commit()

    def connect():
        conn = sqlite3.connect(app.config["DB_PATH"], timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def process_weighing_text(text):
        """按列数分派两列净重 / 三列毛重 CSV，解析并生效；数据问题抛 ValueError。"""
        ncols = sniff_columns(text)
        if ncols == 2:
            items, source = parse_net_csv(text), SOURCE_DIRECT
        elif ncols == 3:
            items, source = parse_gross_csv(text), SOURCE_GROSS
        else:
            raise ValueError("称重 CSV 只支持两列（序号,净重）或三列（序号,毛重,包装批号）")
        with _upload_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = apply_weighings(conn, items, source)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def process_tare_text(text):
        """解析并生效一批「包装批号,皮重」CSV；数据问题抛 ValueError。"""
        items = parse_tare_csv(text)
        with _upload_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = apply_tares(conn, items)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _read_form_text(file_field="file", text_field="csv_text"):
        f = request.files.get(file_field)
        if f is not None and f.filename:
            return f.read().decode("utf-8-sig")
        return request.form.get(text_field, "")

    def _index_context():
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
                "SELECT seq, net_weight, source FROM weighings "
                "ORDER BY seq DESC LIMIT 50").fetchall()
            conversions = conn.execute(
                "SELECT w.seq, w.gross_weight, w.batch_no, p.tare, w.net_weight "
                "FROM weighings w JOIN packaging_batches p "
                "ON p.batch_no = w.batch_no "
                "WHERE w.source = 'gross' ORDER BY w.seq DESC LIMIT 200").fetchall()
            packaging = conn.execute(
                "SELECT batch_no, tare, applied_at, updated_at "
                "FROM packaging_batches ORDER BY batch_no LIMIT 500").fetchall()
            batches = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT 20").fetchall()
        finally:
            conn.close()
        return dict(state=state, weighing_count=weighing_count,
                    event_count=event_count, events=events, recent=recent,
                    conversions=conversions, packaging=packaging, batches=batches,
                    nominal=NOMINAL_GRAMS, trigger=TRIGGER_GRAMS)

    # ------------------------------------------------------------ 页面

    @app.get("/")
    def index():
        return render_template("index.html", **_index_context())

    @app.get("/weighing")
    def lookup():
        raw = request.args.get("seq", "").strip()
        lookup_result, lookup_error = None, None
        try:
            seq = int(raw)
            if seq < 1:
                raise ValueError
        except ValueError:
            lookup_error = f"序号需为正整数：{raw!r}"
        else:
            conn = connect()
            try:
                row = conn.execute(
                    "SELECT w.seq, w.net_weight, w.gross_weight, w.batch_no, "
                    "w.source, p.tare FROM weighings w "
                    "LEFT JOIN packaging_batches p ON p.batch_no = w.batch_no "
                    "WHERE w.seq = ?", (seq,)).fetchone()
            finally:
                conn.close()
            if row is None:
                lookup_error = f"查无序号 {seq}"
            else:
                lookup_result = row
        return render_template(
            "index.html", lookup_result=lookup_result,
            lookup_error=lookup_error, lookup_seq=raw, **_index_context())

    @app.post("/upload")
    def upload():
        try:
            result = process_weighing_text(_read_form_text())
        except ValueError as exc:
            flash(f"上传未生效：{exc}", "error")
            return redirect(url_for("index"))
        except Exception:
            app.logger.exception("上传处理失败")
            flash("内部错误，本次上传未生效", "error")
            return redirect(url_for("index"))
        kind = "三列毛重" if result["op_type"] == SOURCE_GROSS else "两列净重"
        flash("批次 #{}（{}）已生效：新增 {} 条、更正 {} 条{}".format(
            result["batch_id"], kind, result["additions"], result["corrections"],
            _recompute_note(result)), "ok")
        return redirect(url_for("index"))

    @app.post("/tare")
    def tare_upload():
        try:
            result = process_tare_text(_read_form_text("tare_file", "tare_text"))
        except ValueError as exc:
            flash(f"皮重登记未生效：{exc}", "error")
            return redirect(url_for("index"))
        except Exception:
            app.logger.exception("皮重处理失败")
            flash("内部错误，本次皮重登记未生效", "error")
            return redirect(url_for("index"))
        affected = "、".join(str(s) for s in result["affected_seqs"]) or "无"
        flash("皮重批次 #{} 已生效：新登记 {} 个包装批、更正 {} 个；"
              "重新换算序号：{}{}".format(
                  result["batch_id"], result["additions"], result["corrections"],
                  affected, _recompute_note(result)), "ok")
        return redirect(url_for("index"))

    # ------------------------------------------------------------ API

    @app.post("/api/upload")
    def api_upload():
        text = request.get_data(as_text=True)
        try:
            result = process_weighing_text(text)
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("上传处理失败")
            return jsonify(ok=False, error="内部错误，本次上传未生效"), 500
        return jsonify(ok=True, **result)

    @app.post("/api/tare")
    def api_tare():
        text = request.get_data(as_text=True)
        try:
            result = process_tare_text(text)
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("皮重处理失败")
            return jsonify(ok=False, error="内部错误，本次皮重登记未生效"), 500
        return jsonify(ok=True, **result)

    @app.get("/api/weighing/<int:seq>")
    def api_weighing(seq):
        conn = connect()
        try:
            row = conn.execute(
                "SELECT w.seq, w.net_weight, w.gross_weight, w.batch_no, "
                "w.source, p.tare FROM weighings w "
                "LEFT JOIN packaging_batches p ON p.batch_no = w.batch_no "
                "WHERE w.seq = ?", (seq,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return jsonify(ok=False, error=f"查无序号 {seq}"), 404
        return jsonify(ok=True, **dict(row))

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
                "SELECT seq, net_weight, gross_weight, batch_no, source "
                "FROM weighings ORDER BY seq LIMIT ?", (limit,)).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/packaging-batches")
    def api_packaging():
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT batch_no, tare, applied_at, updated_at "
                "FROM packaging_batches ORDER BY batch_no").fetchall()
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
